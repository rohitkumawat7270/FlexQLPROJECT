#include "parser/parser.h"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace flexql {

std::string Parser::trim(const std::string &s) {
    size_t a = s.find_first_not_of(" \t\r\n\v\f");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n\v\f");
    return s.substr(a, b - a + 1);
}

std::string Parser::to_upper(const std::string &s) {
    std::string r = s;
    for (auto &c : r) c = toupper((unsigned char)c);
    return r;
}

std::vector<std::string> Parser::tokenize(const std::string &s) {
    std::vector<std::string> tokens;
    size_t i = 0, n = s.size();
    while (i < n) {
        char c = s[i];
        if (isspace((unsigned char)c)) { ++i; continue; }
        // quoted string — strip quotes
        if (c == '\'' || c == '"') {
            char q = c; ++i;
            std::string tok;
            while (i < n && s[i] != q) tok += s[i++];
            if (i < n) ++i;
            tokens.push_back(tok);
            continue;
        }
        if (c == ',' || c == '(' || c == ')' || c == ';') {
            tokens.push_back(std::string(1, c)); ++i; continue;
        }
        // two-char operators
        if (i + 1 < n) {
            std::string two = s.substr(i, 2);
            if (two == "!=" || two == "<>" || two == "<=" || two == ">=") {
                tokens.push_back(two == "<>" ? "!=" : two);
                i += 2; continue;
            }
        }
        if (c == '=' || c == '<' || c == '>') {
            tokens.push_back(std::string(1, c)); ++i; continue;
        }
        // word/number/identifier (including table.col)
        std::string tok;
        while (i < n && !isspace((unsigned char)s[i]) &&
               s[i] != ',' && s[i] != '(' && s[i] != ')' && s[i] != ';' &&
               s[i] != '\'' && s[i] != '"' &&
               s[i] != '=' && s[i] != '<' && s[i] != '>' && s[i] != '!') {
            tok += s[i++];
        }
        if (!tok.empty()) tokens.push_back(tok);
    }
    return tokens;
}

ParsedQuery Parser::parse(const std::string &sql) {
    std::string s = trim(sql);
    if (!s.empty() && s.back() == ';') s.pop_back();
    s = trim(s);

    auto tokens = tokenize(s);
    if (tokens.empty()) {
        ParsedQuery q; q.error = "Empty query"; return q;
    }
    std::string kw = to_upper(tokens[0]);
    if (kw == "CREATE") return parse_create(tokens, s);
    if (kw == "DROP")   return parse_drop(tokens);
    if (kw == "DELETE") return parse_delete(tokens);
    if (kw == "INSERT") return parse_insert(tokens, s);
    if (kw == "SELECT") return parse_select(tokens, s);

    ParsedQuery q;
    q.error = "Unknown command: " + tokens[0] +
              ". Supported: CREATE TABLE, INSERT INTO, SELECT";
    return q;
}

ParsedQuery Parser::parse_create(const std::vector<std::string> &tokens,
                                  const std::string &raw) {
    ParsedQuery q; q.type = QueryType::CREATE_TABLE;
    if (tokens.size() < 3 || to_upper(tokens[1]) != "TABLE") {
        q.error = "Syntax: CREATE TABLE name (col TYPE, ...)"; return q;
    }
    /* Handle: CREATE TABLE IF NOT EXISTS name (...) */
    size_t name_idx = 2;
    if (to_upper(tokens[2]) == "IF" &&
        name_idx+1 < tokens.size() && to_upper(tokens[name_idx+1]) == "NOT" &&
        name_idx+2 < tokens.size() && to_upper(tokens[name_idx+2]) == "EXISTS") {
        q.if_not_exists = true;
        name_idx = 5;
    }
    if (name_idx >= tokens.size()) {
        q.error = "Syntax: CREATE TABLE name (col TYPE, ...)"; return q;
    }
    q.table_name = tokens[name_idx];
    size_t lp = raw.find('('), rp = raw.rfind(')');
    if (lp == std::string::npos || rp == std::string::npos) {
        q.error = "Missing parentheses in CREATE TABLE"; return q;
    }
    std::string body = raw.substr(lp + 1, rp - lp - 1);
    int primary_key_count = 0;
    std::istringstream ss(body);
    std::string col_str;
    while (std::getline(ss, col_str, ',')) {
        col_str = trim(col_str);
        if (col_str.empty()) continue;
        auto toks = tokenize(col_str);
        if (toks.size() < 2) { q.error = "Bad column: " + col_str; return q; }
        ColumnDef cd;
        cd.name = toks[0];
        cd.type = parse_type(toks[1]);
        if (cd.type == DataType::UNKNOWN) {
            q.error = "Unknown type '" + toks[1] + "'. Supported: INT, DECIMAL, VARCHAR, TEXT, DATETIME";
            return q;
        }
        for (size_t i = 2; i < toks.size(); ++i) {
            std::string u = to_upper(toks[i]);
            /* Skip VARCHAR(N) / DECIMAL(p,s) size specifiers */
            if (u == "(") {
                while (i < toks.size() && toks[i] != ")") ++i;
                continue;
            }
            if (u == "NOT" && i+1 < toks.size() && to_upper(toks[i+1]) == "NULL") {
                cd.not_null = true; ++i;
            } else if (u == "PRIMARY" && i+1 < toks.size() && to_upper(toks[i+1]) == "KEY") {
                cd.primary_key = true; cd.not_null = true; ++i;
                ++primary_key_count;
            }
        }
        q.col_defs.push_back(cd);
    }
    if (q.col_defs.empty()) { q.error = "No columns defined"; return q; }
    if (primary_key_count > 1) {
        q.error = "Only one PRIMARY KEY column is supported";
        return q;
    }
    return q;
}

ParsedQuery Parser::parse_drop(const std::vector<std::string> &tokens) {
    ParsedQuery q; q.type = QueryType::DROP_TABLE;
    // Syntax: DROP TABLE name
    if (tokens.size() < 3 || to_upper(tokens[1]) != "TABLE") {
        q.error = "Syntax: DROP TABLE table_name"; return q;
    }
    q.table_name = tokens[2];
    return q;
}

ParsedQuery Parser::parse_delete(const std::vector<std::string> &tokens) {
    ParsedQuery q; q.type = QueryType::DELETE_ROWS;
    /* Syntax: DELETE FROM table_name */
    if (tokens.size() < 3 || to_upper(tokens[1]) != "FROM") {
        q.error = "Syntax: DELETE FROM table_name"; return q;
    }
    q.table_name = tokens[2];
    return q;
}

ParsedQuery Parser::parse_insert(const std::vector<std::string> &tokens,
                                  const std::string &raw) {
    ParsedQuery q; q.type = QueryType::INSERT;
    if (tokens.size() < 3 || to_upper(tokens[1]) != "INTO") {
        q.error = "Syntax: INSERT INTO table VALUES (...)"; return q;
    }
    q.table_name = tokens[2];
    size_t lp = raw.find('(');
    if (lp == std::string::npos) {
        q.error = "Missing parentheses in INSERT"; return q;
    }
    /* Parse one or more value groups: (...), (...), ...
     * Each group becomes one element in multi_row_values. */
    size_t pos = lp;
    size_t raw_len = raw.size();
    while (pos < raw_len) {
        /* Find next '(' */
        while (pos < raw_len && raw[pos] != '(') ++pos;
        if (pos >= raw_len) break;
        ++pos; /* skip '(' */
        std::vector<std::string> group;
        std::string cur;
        bool in_str2 = false;
        char sc2 = 0;
        int depth = 1;
        while (pos < raw_len && depth > 0) {
            char ch = raw[pos++];
            if (in_str2) {
                if (ch == sc2) in_str2 = false;
                else cur += ch;
            } else if (ch == '\'' || ch == '"') {
                in_str2 = true; sc2 = ch;
            } else if (ch == '(') {
                depth++; cur += ch;
            } else if (ch == ')') {
                depth--;
                if (depth == 0) {
                    std::string v2 = trim(cur);
                    if (!v2.empty()) group.push_back(v2);
                    cur.clear();
                } else cur += ch;
            } else if (ch == ',' && depth == 1) {
                std::string v2 = trim(cur);
                if (!v2.empty()) group.push_back(v2);
                cur.clear();
            } else {
                cur += ch;
            }
        }
        if (!group.empty()) q.multi_row_values.push_back(group);
    }
    if (q.multi_row_values.size() == 1)
        q.insert_values = q.multi_row_values[0];
    return q;
}

ParsedQuery Parser::parse_select(const std::vector<std::string> &tokens,
                                  const std::string & /*raw*/) {
    ParsedQuery q; q.type = QueryType::SELECT;
    size_t from_pos = std::string::npos;
    for (size_t i = 1; i < tokens.size(); ++i)
        if (to_upper(tokens[i]) == "FROM") { from_pos = i; break; }
    if (from_pos == std::string::npos) {
        q.error = "Missing FROM in SELECT"; return q;
    }
    std::vector<std::string> col_tokens;
    for (size_t i = 1; i < from_pos; ++i)
        if (tokens[i] != ",") col_tokens.push_back(tokens[i]);
    if (col_tokens.size() == 1 && col_tokens[0] == "*") q.select_star = true;
    else if (col_tokens.empty()) {
        q.error = "Syntax error: no columns specified in SELECT. Use '*' or a column list";
        return q;
    }
    else { q.select_star = false; q.select_cols = col_tokens; }
    if (from_pos + 1 >= tokens.size()) {
        q.error = "Missing table name after FROM"; return q;
    }
    q.from_table = tokens[from_pos + 1];

    auto unsupported_clause = [&](const std::string &token) {
        q.error = "Unsupported clause in SELECT: " + token;
    };

    size_t i = from_pos + 2;
    while (i < tokens.size()) {
        std::string u = to_upper(tokens[i]);
        if (u == "INNER" && i+1 < tokens.size() && to_upper(tokens[i+1]) == "JOIN") {
            if (i+2 >= tokens.size()) { q.error = "Missing table after JOIN"; return q; }
            q.join.present = true; q.join.table_b = tokens[i+2]; i += 3;
            while (i < tokens.size() && to_upper(tokens[i]) != "ON") ++i;
            if (i >= tokens.size()) { q.error = "Missing ON in JOIN"; return q; }
            ++i;
            if (i+2 >= tokens.size()) { q.error = "Malformed JOIN ON clause"; return q; }
            q.join.col_a = tokens[i]; q.join.col_b = tokens[i+2]; i += 3;
        } else if (u == "WHERE") {
            if (i+3 > tokens.size()) { q.error = "Malformed WHERE clause"; return q; }
            q.where.present = true;
            q.where.column  = tokens[i+1];
            q.where.op      = tokens[i+2];
            q.where.value   = tokens[i+3];
            i += 4;
            // Check for AND / OR — not supported
            if (i < tokens.size()) {
                std::string next = to_upper(tokens[i]);
                if (next == "AND" || next == "OR") {
                    q.error = "ERROR: Only one WHERE condition supported. AND/OR are not allowed";
                    return q;
                }
            }
        } else if (u == "ORDER" && i+1 < tokens.size() && to_upper(tokens[i+1]) == "BY") {
            q.error = "ORDER BY is not supported in this assignment";
            return q;
        } else if (u == "GROUP" && i+1 < tokens.size() && to_upper(tokens[i+1]) == "BY") {
            q.error = "GROUP BY is not supported in this assignment";
            return q;
        } else if (u == "LIMIT") {
            q.error = "LIMIT is not supported in this assignment";
            return q;
        } else {
            unsupported_clause(tokens[i]);
            return q;
        }
    }
    return q;
}

} // namespace flexql
