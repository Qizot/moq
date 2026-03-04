use std::collections::VecDeque;
use std::pin::Pin;

use buf_list::BufList;
use futures::{StreamExt, stream::FuturesUnordered};

use super::{Frame, Timestamp};
use crate::Error;

/// A consumer for hang-formatted media tracks with timestamp reordering.
///
/// This wraps a `moq_lite::TrackConsumer` and adds hang-specific functionality
/// like timestamp decoding, latency management, and frame buffering.
///
/// ## Latency Management
///
/// The consumer can skip groups that are too far behind to maintain low latency.
/// Configure the maximum acceptable delay through the consumer's latency settings.
pub struct OrderedConsumer {
	pub track: moq_lite::TrackConsumer,

	// The current group that we are reading from.
	current: Option<GroupReader>,

	// Future groups that we are monitoring, deciding based on [latency] whether to skip.
	pending: VecDeque<GroupReader>,

	// The maximum timestamp seen thus far, or zero because that's easier than None.
	max_timestamp: Timestamp,

	// The maximum buffer size before skipping a group.
	max_latency: std::time::Duration,

	// The expected next group sequence number after consuming a group.
	next_sequence: Option<u64>,

	// Timeout for waiting on a missing sequence before skipping the gap.
	pending_timeout: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl OrderedConsumer {
	/// Create a new OrderedConsumer wrapping the given moq-lite consumer.
	pub fn new(track: moq_lite::TrackConsumer, max_latency: std::time::Duration) -> Self {
		Self {
			track,
			current: None,
			pending: VecDeque::new(),
			max_timestamp: Timestamp::default(),
			max_latency,
			next_sequence: None,
			pending_timeout: None,
		}
	}

	/// Read the next frame from the track.
	///
	/// This method handles timestamp decoding, group ordering, and latency management
	/// automatically. It will skip groups that are too far behind to maintain the
	/// configured latency target.
	///
	/// Returns `None` when the track has ended.
	pub async fn read(&mut self) -> Result<Option<Frame>, Error> {
		let latency = self.max_latency.try_into()?;
		loop {
			// Try to promote from pending to current when there's no gap.
			if self.current.is_none() {
				let should_promote = match self.next_sequence {
					Some(expected) => self.pending.front().is_some_and(|g| g.info.sequence == expected),
					None => !self.pending.is_empty(), // first group ever
				};
				if should_promote {
					self.pending_timeout = None;
					self.current = self.pending.pop_front();
					continue;
				}
			}

			let cutoff = self.max_timestamp.checked_add(latency)?;

			// Keep track of all pending groups, buffering until we detect a timestamp far enough in the future.
			// This is a race; only the first group will succeed.
			// TODO is there a way to do this without FuturesUnordered?
			let mut buffering = FuturesUnordered::new();
			for (index, pending) in self.pending.iter_mut().enumerate() {
				buffering.push(async move { (index, pending.buffer_until(cutoff).await) })
			}

			tokio::select! {
				biased;
				Some(res) = async { Some(self.current.as_mut()?.read().await) } => {
					drop(buffering);

					match res {
						// Got the next frame.
						Ok(Some(frame)) => {
							self.max_timestamp = self.max_timestamp.max(frame.timestamp);
							return Ok(Some(frame));
						}
						Ok(None) | Err(_) => {
							// Group ended, update expected sequence.
							// We don't care about errors, which will happen if the group is closed early.
							// Let promotion logic at loop top decide the next current.
							self.next_sequence = self.current.as_ref().map(|c| c.info.sequence + 1);
							self.pending_timeout = None;
							self.current = None;
							continue;
						}
					};
				},
				Some(res) = async { self.track.next_group().await.transpose() } => {
					let group = GroupReader::new(res?);
					drop(buffering);

					match self.current.as_ref() {
						Some(current) if group.info.sequence < current.info.sequence => {
							// Ignore old groups
							tracing::debug!(old = ?group.info.sequence, current = ?current.info.sequence, "skipping old group");
						},
						Some(_) => {
							// Insert into pending based on the sequence number ascending.
							let index = self.pending.partition_point(|g| g.info.sequence < group.info.sequence);
							self.pending.insert(index, group);
						},
						None => {
							// Always insert sorted by sequence; promotion happens at loop top.
							let index = self.pending.partition_point(|g| g.info.sequence < group.info.sequence);
							self.pending.insert(index, group);
							if self.pending_timeout.is_none() {
								self.pending_timeout = Some(Box::pin(tokio::time::sleep(self.max_latency)));
							}
						}
					};
				},
				Some((index, timestamp)) = buffering.next() => {
					if self.current.is_some() {
						tracing::debug!(old = ?self.max_timestamp, new = ?timestamp, buffer = ?self.max_latency, "skipping slow group");
					}

					drop(buffering);

					if index > 0 {
						self.pending.drain(0..index);
						tracing::debug!(count = index, "skipping additional groups");
					}

					self.current = self.pending.pop_front();
					self.next_sequence = self.current.as_ref().map(|c| c.info.sequence + 1);
					self.pending_timeout = None;
				}
				// Timeout waiting for a missing sequence — skip the gap.
				() = async {
					match &mut self.pending_timeout {
						Some(sleep) => sleep.as_mut().await,
						None => std::future::pending().await,
					}
				} => {
					drop(buffering);
					self.pending_timeout = None;
					self.next_sequence = self.pending.front().map(|g| g.info.sequence);
					self.current = self.pending.pop_front();
					continue;
				}
				else => return Ok(None),
			}
		}
	}

	/// Set the maximum latency tolerance for this consumer.
	///
	/// Groups with timestamps older than `max_timestamp - max_latency` will be skipped.
	pub fn set_max_latency(&mut self, max: std::time::Duration) {
		self.max_latency = max;
	}

	/// Wait until the track is closed.
	pub async fn closed(&self) -> Result<(), Error> {
		Ok(self.track.closed().await?)
	}
}

impl From<OrderedConsumer> for moq_lite::TrackConsumer {
	fn from(inner: OrderedConsumer) -> Self {
		inner.track
	}
}

impl std::ops::Deref for OrderedConsumer {
	type Target = moq_lite::TrackConsumer;

	fn deref(&self) -> &Self::Target {
		&self.track
	}
}

/// Internal reader for a group of frames.
struct GroupReader {
	// The group.
	group: moq_lite::GroupConsumer,

	// The current frame index
	index: usize,

	// The any buffered frames in the group.
	buffered: VecDeque<Frame>,

	// The max timestamp in the group
	max_timestamp: Option<Timestamp>,
}

impl GroupReader {
	fn new(group: moq_lite::GroupConsumer) -> Self {
		Self {
			group,
			index: 0,
			buffered: VecDeque::new(),
			max_timestamp: None,
		}
	}

	async fn read(&mut self) -> Result<Option<Frame>, Error> {
		if let Some(frame) = self.buffered.pop_front() {
			Ok(Some(frame))
		} else {
			self.read_unbuffered().await
		}
	}

	async fn read_unbuffered(&mut self) -> Result<Option<Frame>, Error> {
		// Cancel-safe: get_frame() does not advance the group's index.
		// If this future is dropped before we increment self.index,
		// the next call will retry the same frame.
		let Some(mut frame) = self.group.get_frame(self.index).await? else {
			return Ok(None);
		};
		let payload = frame.read_chunks().await?;

		let mut payload = BufList::from_iter(payload);

		let timestamp = Timestamp::decode(&mut payload)?;

		let frame = Frame {
			keyframe: (self.index == 0),
			timestamp,
			payload,
		};

		// Only advance after full success — this is the cancel-safety guarantee.
		self.index += 1;
		self.max_timestamp = Some(self.max_timestamp.unwrap_or_default().max(timestamp));

		Ok(Some(frame))
	}

	// Keep reading and buffering new frames, returning when `max` is larger than or equal to the cutoff.
	// This will BLOCK FOREVER if the group has ended early; it's intended to be used within select!
	async fn buffer_until(&mut self, cutoff: Timestamp) -> Timestamp {
		loop {
			match self.max_timestamp {
				Some(timestamp) if timestamp >= cutoff => return timestamp,
				_ => (),
			}

			match self.read_unbuffered().await {
				Ok(Some(frame)) => self.buffered.push_back(frame),
				// Otherwise block forever so we don't return from FuturesUnordered
				_ => std::future::pending().await,
			}
		}
	}
}

impl std::ops::Deref for GroupReader {
	type Target = moq_lite::GroupConsumer;

	fn deref(&self) -> &Self::Target {
		&self.group
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;

	/// Write a single audio frame as its own group with the given sequence and timestamp.
	fn write_audio_group(track: &mut moq_lite::TrackProducer, sequence: u64, timestamp_us: u64) {
		let mut group = track.create_group(moq_lite::Group { sequence }).unwrap();
		let frame = Frame {
			timestamp: Timestamp::from_micros(timestamp_us).unwrap(),
			keyframe: true,
			payload: BufList::from(bytes::Bytes::from_static(b"audio")),
		};
		frame.encode(&mut group).unwrap();
		group.finish().unwrap();
	}

	#[tokio::test(start_paused = true)]
	async fn audio_in_order() {
		let mut track = moq_lite::Track::new("audio").produce();
		let consumer = track.consume();
		let mut consumer = OrderedConsumer::new(consumer, Duration::from_millis(500));

		write_audio_group(&mut track, 0, 0);
		write_audio_group(&mut track, 1, 20_000);
		write_audio_group(&mut track, 2, 40_000);
		track.finish().unwrap();

		let f0 = consumer.read().await.unwrap().unwrap();
		let f1 = consumer.read().await.unwrap().unwrap();
		let f2 = consumer.read().await.unwrap().unwrap();

		assert_eq!(f0.timestamp.as_micros(), 0);
		assert_eq!(f1.timestamp.as_micros(), 20_000);
		assert_eq!(f2.timestamp.as_micros(), 40_000);

		// Timestamps are monotonically increasing
		assert!(f0.timestamp <= f1.timestamp);
		assert!(f1.timestamp <= f2.timestamp);
	}

	#[tokio::test(start_paused = true)]
	async fn audio_out_of_order_reorders() {
		let mut track = moq_lite::Track::new("audio").produce();
		let consumer = track.consume();
		let mut consumer = OrderedConsumer::new(consumer, Duration::from_millis(500));

		// Write seq 0, read it to establish next_sequence = 1
		write_audio_group(&mut track, 0, 0);
		let f0 = consumer.read().await.unwrap().unwrap();
		assert_eq!(f0.timestamp.as_micros(), 0);

		// Write seq 2 first (out of order), then seq 1 (fills gap)
		write_audio_group(&mut track, 2, 40_000);
		write_audio_group(&mut track, 1, 20_000);

		let f1 = consumer.read().await.unwrap().unwrap();
		let f2 = consumer.read().await.unwrap().unwrap();

		// Should be delivered in sequence order, not arrival order
		assert_eq!(f1.timestamp.as_micros(), 20_000);
		assert_eq!(f2.timestamp.as_micros(), 40_000);
	}

	#[tokio::test(start_paused = true)]
	async fn audio_missing_group_timeout() {
		let mut track = moq_lite::Track::new("audio").produce();
		let consumer = track.consume();
		let max_latency = Duration::from_millis(100);
		let mut consumer = OrderedConsumer::new(consumer, max_latency);

		// Write and read seq 0
		write_audio_group(&mut track, 0, 0);
		let f0 = consumer.read().await.unwrap().unwrap();
		assert_eq!(f0.timestamp.as_micros(), 0);

		// Write seq 2 — gap at seq 1, never filled
		write_audio_group(&mut track, 2, 40_000);

		// Consumer should NOT resolve immediately (waiting for seq 1)
		let result = tokio::time::timeout(Duration::from_millis(50), consumer.read()).await;
		assert!(result.is_err(), "should be waiting for missing seq 1");

		// Advance past the timeout
		tokio::time::advance(Duration::from_millis(60)).await;

		// Now seq 2 should be delivered (skipped the gap)
		let f2 = consumer.read().await.unwrap().unwrap();
		assert_eq!(f2.timestamp.as_micros(), 40_000);
	}

	#[tokio::test(start_paused = true)]
	async fn audio_multiple_gaps() {
		let mut track = moq_lite::Track::new("audio").produce();
		let consumer = track.consume();
		let mut consumer = OrderedConsumer::new(consumer, Duration::from_millis(500));

		// Write and read seq 0
		write_audio_group(&mut track, 0, 0);
		let f0 = consumer.read().await.unwrap().unwrap();
		assert_eq!(f0.timestamp.as_micros(), 0);

		// Write seq 3, seq 1, seq 2 (arrive in this order from network)
		write_audio_group(&mut track, 3, 60_000);
		write_audio_group(&mut track, 1, 20_000);
		write_audio_group(&mut track, 2, 40_000);

		let f1 = consumer.read().await.unwrap().unwrap();
		let f2 = consumer.read().await.unwrap().unwrap();
		let f3 = consumer.read().await.unwrap().unwrap();

		// Should be fully reordered
		assert_eq!(f1.timestamp.as_micros(), 20_000);
		assert_eq!(f2.timestamp.as_micros(), 40_000);
		assert_eq!(f3.timestamp.as_micros(), 60_000);
	}

	#[tokio::test(start_paused = true)]
	async fn video_multiframe_group_unchanged() {
		let track = moq_lite::Track::new("video").produce();
		let consumer = track.consume();
		let mut consumer = OrderedConsumer::new(consumer, Duration::from_millis(500));

		// Write a multi-frame group via OrderedProducer
		let mut producer = super::super::OrderedProducer::new(track);
		producer
			.write(Frame {
				timestamp: Timestamp::from_micros(0).unwrap(),
				keyframe: true,
				payload: BufList::from(bytes::Bytes::from_static(b"keyframe")),
			})
			.unwrap();
		producer
			.write(Frame {
				timestamp: Timestamp::from_micros(16_000).unwrap(),
				keyframe: false,
				payload: BufList::from(bytes::Bytes::from_static(b"inter1")),
			})
			.unwrap();
		producer
			.write(Frame {
				timestamp: Timestamp::from_micros(33_000).unwrap(),
				keyframe: false,
				payload: BufList::from(bytes::Bytes::from_static(b"inter2")),
			})
			.unwrap();
		producer.flush().unwrap();

		let f0 = consumer.read().await.unwrap().unwrap();
		let f1 = consumer.read().await.unwrap().unwrap();
		let f2 = consumer.read().await.unwrap().unwrap();

		assert!(f0.keyframe);
		assert!(!f1.keyframe);
		assert!(!f2.keyframe);

		assert_eq!(f0.timestamp.as_micros(), 0);
		assert_eq!(f1.timestamp.as_micros(), 16_000);
		assert_eq!(f2.timestamp.as_micros(), 33_000);
	}
}
