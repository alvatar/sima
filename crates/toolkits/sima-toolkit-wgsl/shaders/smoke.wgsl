// Exercises the toolkit path end to end: two group-0 storage buffers, one
// invocation per element, doubling each value and adding one. The bounds guard
// lets the launch round the workgroup count up past the element count.

@group(0) @binding(0) var<storage, read> in_buf: array<u32>;
@group(0) @binding(1) var<storage, read_write> out_buf: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= arrayLength(&out_buf)) {
        return;
    }
    out_buf[i] = in_buf[i] * 2u + 1u;
}
