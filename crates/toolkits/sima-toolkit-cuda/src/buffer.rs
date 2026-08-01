//! Device allocations and host/device transfers.

use cudarc::driver::CudaSlice;

use sima_core::{Error, Result};

use crate::context::Context;
use crate::driver;

/// A device allocation of untyped bytes, freed on drop.
///
/// The allocation is bytes rather than a typed element sequence because a
/// kernel's parameters are whatever its own declaration says they are: the same
/// buffer carries `f32` grid cells to one kernel and `u32` dimensions to
/// another, and the toolkit stays out of that.
pub struct Buffer {
    bytes: CudaSlice<u8>,
}

impl Buffer {
    /// The allocation, for binding as a kernel parameter.
    pub(crate) fn bytes(&self) -> &CudaSlice<u8> {
        &self.bytes
    }

    /// The allocation's size in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the allocation is empty. Never true: [`Context::buffer`] rejects
    /// a zero-sized request.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl Context {
    /// Allocates `size` bytes on the device, zeroed.
    ///
    /// Zeroing costs one device-side fill per allocation and buys a defined
    /// starting value, so a kernel reading a byte no upload covered reads a
    /// zero rather than whatever the last allocation left there. `size` must be
    /// greater than zero.
    pub fn buffer(&self, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Err(Error::Backend(
                "buffer size must be greater than zero".to_string(),
            ));
        }
        let bytes = self
            .stream()
            .alloc_zeros::<u8>(size)
            .map_err(|e| driver::backend_error("allocate device memory", e))?;
        Ok(Buffer { bytes })
    }

    /// Copies host bytes into the start of a device allocation.
    ///
    /// `data` must not exceed the destination's size. The copy is submitted to
    /// the context's stream, which orders it against the dispatches around it.
    pub fn upload(&self, dst: &mut Buffer, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if data.len() > dst.bytes.len() {
            return Err(Error::Backend(format!(
                "upload of {} bytes exceeds buffer size {}",
                data.len(),
                dst.bytes.len()
            )));
        }
        // The copy carries `data.len()` bytes into the head of the allocation,
        // so a buffer larger than what it is given keeps the rest of its
        // contents — the dimensions and parameter buffers a dispatch binds are
        // written this way.
        self.stream()
            .memcpy_htod(data, &mut dst.bytes)
            .map_err(|e| driver::backend_error("copy host bytes to the device", e))
    }

    /// Copies a device allocation back to host bytes, returning its full
    /// contents.
    ///
    /// The stream is drained before the bytes are read, so every dispatch and
    /// upload submitted before this call has landed.
    pub fn download(&self, src: &Buffer) -> Result<Vec<u8>> {
        let mut out = vec![0u8; src.bytes.len()];
        self.stream()
            .memcpy_dtoh(&src.bytes, out.as_mut_slice())
            .map_err(|e| driver::backend_error("copy device bytes to the host", e))?;
        self.stream()
            .synchronize()
            .map_err(|e| driver::backend_error("drain the stream after a readback", e))?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    /// Every buffer test allocates through a context, so each one needs an
    /// NVIDIA device.
    mod on_device {
        use super::super::*;

        #[test]
        fn buffer_rejects_zero_size() {
            let context = Context::new().expect("create compute context");
            assert!(matches!(context.buffer(0), Err(Error::Backend(_))));
        }

        #[test]
        fn buffer_round_trips_bytes() {
            let context = Context::new().expect("create compute context");
            let data: Vec<u8> = (0..=255).collect();
            let mut buffer = context.buffer(data.len()).expect("allocate buffer");
            context.upload(&mut buffer, &data).expect("upload");
            let read_back = context.download(&buffer).expect("download");
            assert_eq!(read_back, data);
        }

        #[test]
        fn a_fresh_buffer_reads_back_zeroed() {
            let context = Context::new().expect("create compute context");
            let buffer = context.buffer(64).expect("allocate buffer");
            assert_eq!(context.download(&buffer).expect("download"), vec![0u8; 64]);
        }

        #[test]
        fn an_upload_larger_than_its_destination_is_rejected() {
            let context = Context::new().expect("create compute context");
            let mut buffer = context.buffer(4).expect("allocate buffer");
            assert!(matches!(
                context.upload(&mut buffer, &[0u8; 8]),
                Err(Error::Backend(_))
            ));
        }

        #[test]
        fn a_partial_upload_leaves_the_tail_untouched() {
            // Uploads are sized by what they carry, not by the destination, because
            // the dimensions and parameter buffers a dispatch binds are smaller
            // than the grid buffers beside them.
            let context = Context::new().expect("create compute context");
            let mut buffer = context.buffer(8).expect("allocate buffer");
            context.upload(&mut buffer, &[1, 2, 3, 4]).expect("upload");
            assert_eq!(
                context.download(&buffer).expect("download"),
                vec![1, 2, 3, 4, 0, 0, 0, 0]
            );
        }
    }
}
