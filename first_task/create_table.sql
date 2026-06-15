CREATE TABLE `transactions_v2` (
    call_id         Utf8,
    call_time       Utf8,
    client_id       Utf8,
    region_code     Utf8,
    campaign_type   Utf8,
    call_status     Utf8,
    client_response Utf8,
    duration_sec    Int32,
    follow_up_required Utf8,
    PRIMARY KEY (call_id)
);
