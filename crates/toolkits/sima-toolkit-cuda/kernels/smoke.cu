// Exercises the toolkit path end to end: two buffers of values and one holding
// the element count, one thread per element, doubling each value and adding
// one. The bounds guard lets the launch round the block count up past the
// element count.
//
// Parameters bind positionally, in declaration order, and the block width the
// launch uses is the one `__launch_bounds__` states here.

extern "C" __global__ void __launch_bounds__(64) main_kernel(
    const unsigned int* in_buf,
    unsigned int* out_buf,
    const unsigned int* count) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count[0]) {
        return;
    }
    out_buf[i] = in_buf[i] * 2u + 1u;
}
