#[restate_sdk::service]
#[name = "OpenDAL"]
pub trait Service {
    /// List entries in a given location.
    async fn list(request: Json<ListRequest>) -> HandlerResult<Json<ListResponse>>;

    /// Presign an operation for read.
    #[name = "presignRead"]
    async fn presign_read(
        request: Json<PresignReadRequest>,
    ) -> HandlerResult<Json<PresignResponse>>;

    /// Presign an operation for read.
    #[name = "presignStat"]
    async fn presign_stat(
        request: Json<PresignStatRequest>,
    ) -> HandlerResult<Json<PresignResponse>>;
}

pub type ListRequest = service::ListRequest<Location>;
pub type PresignReadRequest = service::PresignRequest<Location, ReadOptions>;
pub type PresignStatRequest = service::PresignRequest<Location, StatOptions>;

handler_impl!(list);
handler_impl!(presign_read, PresignResponse);
handler_impl!(presign_stat, PresignResponse);
