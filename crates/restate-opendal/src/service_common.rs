pub type ListRequest = service::ListRequest<Location>;
pub type PresignReadRequest = service::PresignRequest<Location, ReadOptions>;
pub type PresignStatRequest = service::PresignRequest<Location, StatOptions>;

handler_impl!(list);
handler_impl!(presign_read, PresignResponse);
handler_impl!(presign_stat, PresignResponse);
