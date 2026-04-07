#pragma once
#include <string>
#include <cstdint>

/*
 * Wire protocol (text-based, newline-delimited frames):
 *
 * Client -> Server:
 *   LENGTH\n<sql bytes>
 *
 * Server -> Client:
 *   STATUS\n          (OK or ERR)
 *   [ERRMSG\n<msg>\n] (only on ERR)
 *   COLS\n<n>\n<col1>\n<col2>\n...
 *   ROWS\n<r>\n
 *     for each row: NVALS\n<n>\n<v1>\n<v2>\n...<vn>\n<expiry>\n
 *   END\n
 */

namespace flexql {
namespace proto {

const std::string STATUS_OK  = "OK";
const std::string STATUS_ERR = "ERR";
const std::string TAG_COLS   = "COLS";
const std::string TAG_ROWS   = "ROWS";
const std::string TAG_NVALS  = "NVALS";
const std::string TAG_END    = "END";
const std::string TAG_ERRMSG = "ERRMSG";

bool send_all(int fd, const std::string &data);
bool recv_line(int fd, std::string &line);
bool recv_n(int fd, char *buf, size_t n);

} // proto
} // flexql
