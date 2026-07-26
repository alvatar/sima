//! The concrete CA models. Each is a [`CaModel`](super::model::CaModel)
//! implementation in its own module; adding one is a new module here plus a
//! registry arm in [`super`].

pub(crate) mod gray_scott;
pub(crate) mod gray_scott_cuda;
pub(crate) mod nca;
