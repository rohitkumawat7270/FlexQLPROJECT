#pragma once
/*
 * Expiration timestamps are stored per row (time_t).
 * The Table::is_expired() method checks on read.
 * For proactive cleanup a background thread can be added.
 * This header documents the design.
 *
 * INSERT syntax extended:
 *   INSERT INTO t VALUES (v1, v2, ..., <unix_epoch>);
 * If the last value is larger than any reasonable column value AND
 * it is numeric, it is treated as an expiration timestamp.
 * The client/user explicitly appends the epoch as the last value.
 *
 * Example:
 *   INSERT INTO session VALUES (42, 'token_abc', 1893456000);
 *   -- row expires at Unix time 1893456000
 */
namespace flexql {
// Expiration logic lives inside Table::is_expired() (storage/table.cpp)
// This file is intentionally minimal.
}
