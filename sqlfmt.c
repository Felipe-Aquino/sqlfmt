#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <stdarg.h>
#include <assert.h>

#define DARRAY_IMPLEMENTATION
#include "darray.h"

#define MIN(a, b) ((a) > (b) ? (b) : (a))

typedef struct StringView {
    const char *data;
    int size;
} StringView;


#define IS_NEWLINE(c) (c) == '\n'
#define IS_SPACE(c) ((c) == ' ' || (c) == '\t' || (c) == '\r' || IS_NEWLINE(c))
#define IS_ALPHA(c) (('a' <= (c) && (c) <= 'z') || ('A' <= (c) && (c) <= 'Z'))
#define IS_DIGIT(c) ('0' <= (c) && (c) <= '9')
#define IS_ALNUM(c) (IS_ALPHA(c) || IS_DIGIT(c) || (c == '_'))

#define SVFmt "%.*s"
#define SVArg(sv) (sv).size, (sv).data

StringView sv_make(const char *data, int size);
StringView sv_make2(const char *data);
long sv_offset(StringView v1, StringView v2);
bool sv_equal(StringView v1, StringView v2);
bool sv_equal_i(StringView v1, StringView v2); // Case insensitive
StringView sv_chop_str(StringView *str, int amount);
StringView sv_skip(StringView str, int amount);
StringView sv_skip_spaces(StringView str);
bool sv_is_empty(StringView str);
StringView sv_read_until_char(StringView str, char c);
StringView sv_read_while_alnum(StringView str);
StringView sv_read_while_alpha(StringView str);
StringView sv_read_while_number(StringView str);
StringView sv_read_file(const char *filename);

typedef enum TokenType {
    TT_EOF = 0,
    TT_UNKNOWN,
    TT_IDENTIFIER,
    TT_NUMBER,
    TT_STRING,
    TT_ATTRIB,
    TT_PLUS,
    TT_MINUS,
    TT_SLASH,
    TT_STAR,
    TT_BOOL_OP,
    TT_AND,
    TT_OR,
    TT_LIKE,
    TT_ILIKE,
    TT_IN,
    TT_NOT_IN,
    TT_OPEN_PAREN,
    TT_CLOSE_PAREN,
    TT_SEMI,
    TT_COMMA,
    TT_SELECT,
    TT_INNER_JOIN,
    TT_LEFT_JOIN,
    TT_RIGHT_JOIN,
    TT_LEFT_OUTER_JOIN,
    TT_RIGHT_OUTER_JOIN,
    TT_ON,
    TT_WHERE,
    TT_IS_NOT,
    TT_IS,
    TT_ORDER_BY,
    TT_GROUP_BY,
    TT_HAVING,
    TT_ASC,
    TT_DESC,
    TT_LIMIT,
    TT_OFFSET,
    TT_INSERT_INTO,
    TT_DELETE_FROM,
    TT_VALUES,
    TT_UPDATE,
    TT_SET,
    TT_FROM,
    TT_TOKEN_COUNT,
} TokenType;

static_assert(TT_TOKEN_COUNT == 44);

const char * getTokenTypeStr(TokenType t) {
    switch (t) {
    case TT_EOF: return "TT_EOF";
    case TT_UNKNOWN: return "TT_UNKNOWN";
    case TT_IDENTIFIER: return "TT_IDENTIFIER";
    case TT_NUMBER: return "TT_NUMBER";
    case TT_STRING: return "TT_STRING";
    case TT_ATTRIB: return "TT_ATTRIB";
    case TT_PLUS: return "TT_PLUS";
    case TT_MINUS: return "TT_MINUS";
    case TT_SLASH: return "TT_SLASH";
    case TT_STAR: return "TT_STAR";
    case TT_BOOL_OP: return "TT_BOOL_OP";
    case TT_AND: return "TT_AND";
    case TT_OR: return "TT_OR";
    case TT_LIKE: return "TT_LIKE";
    case TT_ILIKE: return "TT_ILIKE";
    case TT_IN: return "TT_IN";
    case TT_NOT_IN: return "TT_NOT_IN";
    case TT_OPEN_PAREN: return "TT_OPEN_PAREN";
    case TT_CLOSE_PAREN: return "TT_CLOSE_PAREN";
    case TT_SEMI: return "TT_SEMI";
    case TT_COMMA: return "TT_COMMA";
    case TT_SELECT: return "TT_SELECT";
    case TT_INNER_JOIN: return "TT_INNER_JOIN";
    case TT_LEFT_JOIN: return "TT_LEFT_JOIN";
    case TT_RIGHT_JOIN: return "TT_RIGHT_JOIN";
    case TT_LEFT_OUTER_JOIN: return "TT_LEFT_OUTER_JOIN";
    case TT_RIGHT_OUTER_JOIN : return "TT_RIGHT_OUTER_JOIN";
    case TT_ON: return "TT_ON";
    case TT_WHERE: return "TT_WHERE";
    case TT_IS_NOT: return "TT_IS_NOT";
    case TT_IS: return "TT_IS";
    case TT_ORDER_BY: return "TT_ORDER_BY";
    case TT_GROUP_BY: return "TT_GROUP_BY";
    case TT_HAVING: return "TT_HAVING";
    case TT_ASC: return "TT_ASC";
    case TT_DESC: return "TT_DESC";
    case TT_LIMIT: return "TT_LIMIT";
    case TT_OFFSET: return "TT_OFFSET";
    case TT_INSERT_INTO: return "TT_INSERT_INTO";
    case TT_DELETE_FROM: return "TT_DELETE_FROM";
    case TT_VALUES: return "TT_VALUES";
    case TT_UPDATE: return "TT_UPDATE";
    case TT_SET: return "TT_SET";
    case TT_FROM: return "TT_FROM";
    }

    return "??";
}

static const char *keywords_map[][TT_TOKEN_COUNT] = {
    [TT_AND]              = { "and", NULL },
    [TT_OR]               = { "or", NULL },
    [TT_LIKE]             = { "like", NULL },
    [TT_ILIKE]            = { "ilike", NULL },
    [TT_IN]               = { "in", NULL },
    [TT_NOT_IN]           = { "not", "in", NULL },
    [TT_SELECT]           = { "select", NULL },
    [TT_INNER_JOIN]       = { "inner", "join", NULL },
    [TT_LEFT_JOIN]        = { "left", "join", NULL },
    [TT_RIGHT_JOIN]       = { "right", "join", NULL },
    [TT_LEFT_OUTER_JOIN]  = { "left", "outer", "join", NULL },
    [TT_RIGHT_OUTER_JOIN] = { "right", "outer", "join", NULL },
    [TT_ON]               = { "on", NULL },
    [TT_WHERE]            = { "where", NULL },
    [TT_IS_NOT]           = { "is", "not", NULL },
    [TT_IS]               = { "is", NULL },
    [TT_ORDER_BY]         = { "order", "by", NULL },
    [TT_GROUP_BY]         = { "group", "by", NULL },
    [TT_HAVING]           = { "having", NULL },
    [TT_ASC]              = { "asc", NULL },
    [TT_DESC]             = { "desc", NULL },
    [TT_LIMIT]            = { "limit", NULL },
    [TT_OFFSET]           = { "offset", NULL },
    [TT_INSERT_INTO]      = { "insert", "into", NULL },
    [TT_DELETE_FROM]      = { "delete", "from", NULL },
    [TT_VALUES]           = { "values", NULL },
    [TT_UPDATE]           = { "update", NULL },
    [TT_SET]              = { "set", NULL },
    [TT_FROM]             = { "from", NULL },
};

typedef struct Token {
    TokenType type;
    StringView name;
} Token;

typedef struct Tokenizer {
    StringView unchanged;
    StringView data;
    char err_msg[128];
} Tokenizer;

Tokenizer tk_make(const char *text);
Token tk_make_identifier_token(Tokenizer *tokenizer, StringView data);
bool tk_read_identifier(Tokenizer *tokenizer, Token *token);
bool tk_read_number(Tokenizer *tokenizer, Token *token);
bool tk_read_string(Tokenizer *tokenizer, Token *token);
bool tk_next_token(Tokenizer *tokenizer, Token *token);

typedef enum StmtType {
    ST_SELECT,
    ST_INSERT,
    ST_UPDATE,
    ST_DELETE,
} StmtType;

/* only TT_PLUS, TT_MINUS, TT_SLASH, TT_STAR, TT_BOOL_OP,
 *      TT_AND, TT_OR,
 *      TT_IN, TT_LIKE, TT_ILIKE,
 *      TT_IS, TT_IS_NOT
 *      TT_STRING, TT_NUMBER, TT_IDENTIFIER
 **/
typedef struct Expr {
    Token token;
    struct Expr *lhs;
    struct Expr *rhs;
    bool parentheses;
} Expr;

typedef struct InExpr {
    Expr base;
    Token *value_list;
} InExpr;

typedef struct AttribExpr {
    Expr base;
    struct AttribExpr *next;
} AttribExpr;

typedef struct TokenList {
    Token value;
    struct TokenList *next;
} TokenList;

typedef struct TokenListList {
    TokenList *list;
    struct TokenListList *next;
} TokenListList;

typedef struct Stmt {
    StmtType type;
} Stmt;

typedef struct DeleteStmt {
    StmtType type;
    Token table_name;
    Expr *where_expr;
} DeleteStmt;

typedef struct InsertStmt {
    StmtType type;
    Token table_name;
    TokenList *fields;
    TokenListList *values_list;
} InsertStmt;

typedef struct UpdateStmt {
    StmtType type;
    Token table_name;
    AttribExpr *attribs;
    Expr *where_expr;
} UpdateStmt;

typedef struct GroupByDecl {
    TokenList *fields;
    Expr *having_expr_list;
} GroupByDecl;

typedef struct OrderByDecl {
    bool is_active;
    bool is_desc;
    Token *fields;
} OrderByDecl;

typedef struct JoinDecl {
    Token kind;
    Token table;
    Expr *on_expr_list;
    struct JoinDecl *next;
} JoinDecl;

typedef struct SelectStmt {
    StmtType type;
    Token table_name;
    TokenList *fields;
    JoinDecl *joins;
    GroupByDecl *group_by;
    OrderByDecl order_by;
    Expr *where_expr_list;
    int limit;
    int offset;
} SelectStmt;

/*
 * stmt = select_stmt | insert_stmt | update_stmt | delete_stmt
 *
 * select_stmt = "select" value_stmt (',' value_stmt)* "from" <id>
 *               join_stmt*
 *               where_stmt?
 *               group_by_stmt?
 *               order_by_stmt?
 *               ("limit" number)?
 *               ("offset" number)?
 *               ;?
 *
 * insert_stmt = "insert into" <id> insert_fields? "values" values_list ;?
 *
 * update_stmt = "update" <id> "set" update_list where_stmt? ;?
 *
 * delete_stmt = "delete from" <id> where_stmt? ;?
 *
 * update_list = <id> '=' value_stmt (',' value_stmt)*
 *
 * where_stmt = "where" expr_stmt (("and" | "or") expr_stmt)*
 *
 * group_by_stmt = "group by" <id> (',' <id>)* "having" expr_stmt (("and" | "or") expr_stmt)*
 *
 * order_by_stmt = "order by" <id> (',' <id>)* ("asc" | "desc")?
 *
 * join_flavour = ("inner" | "left" | "right" | "left outer" | "right outer") "join"
 * join_stmt = join_flavour <id> "on" bool_expr (("and" | "or") bool_expr)*
 *
 * arith_op = '+' | '-' | '*' | '/'
 *         
 * bool_op = '=' | '>' | '<' | '>=' | '<=' | '<>' | '!='
 *
 * value_stmt = number | string | <id>
 * bool_expr = value_stmt bool_op value_stmt
 * arith_expr = value_stmt arith_op value_stmt
 *
 * expr_stmt = '(' expr_stmt ')'
 *           | value_stmt
 *           | bool_expr
 *           | arith_expr
 *           | bool_expr ("and" | "or") bool_expr
 *           | value_stmt "like" string
 *           | value_stmt "ilike" string
 *           | value_stmt "in" '(' value_stmt* ')'
 *
 * insert_value = '(' (number | string) * ')'
 * values_list = insert_value (',' insert_value)*
 */

typedef union PoolUnion {
    Expr e;
    InExpr ie;
    AttribExpr ae;
    TokenList tl;
    TokenListList tll;
    DeleteStmt ds;
    InsertStmt is;
    UpdateStmt us;
    SelectStmt ss;
    JoinDecl jd;
    GroupByDecl gd;
    OrderByDecl od;
} PoolUnion;

typedef struct Parser {
    Token *tokens;
    int token_idx;

    Stmt *stmt;

    void *alloc_pool;
} Parser;

Parser parser_make(Token *tokens) {
    int token_count = darray_length(tokens);

    Parser parser;
    parser.tokens = tokens;
    parser.token_idx = 0;
    parser.stmt = NULL;

    // Assuming this is enough for no more allocation be needed,
    // because is expected the number of tokens to be greater then the number
    // or statements and expressions.
    //
    // This is important because the pointers can reference free'd memory if a
    // reallocation occour.
    parser.alloc_pool = darray_reserve(PoolUnion, token_count * 2);

    return parser;
}

void parser_clear(Parser *parser) {
    darray_destroy(parser->tokens);
    darray_destroy(parser->alloc_pool);
}

Token parser_current_token(Parser *parser) {
    return parser->tokens[parser->token_idx];
}

Token parser_next_token(Parser *parser) {
    return parser->tokens[++parser->token_idx];
}

Token parser_peek_next_token(Parser *parser) {
    return parser->tokens[parser->token_idx + 1];
}

#define parser_alloc(parser, value) \
    ({ darray_push(parser->alloc_pool, value); darray_last(parser->alloc_pool); })

bool tt_isoneof(TokenType type, int count, ...) {
    va_list args;
    va_start(args, count);

    for (int i = 0; i < count; ++i) {
        TokenType tt = va_arg(args, TokenType);

        if (type == tt) {
            va_end(args);
            return true;
        }
    }

    va_end(args);

    return false;
}

bool tt_isoneof_array(TokenType type, int count, TokenType *expected_types) {
    for (int i = 0; i < count; ++i) {
        TokenType tt = expected_types[i];

        if (type == tt) {
            return true;
        }
    }

    return false;
}

bool parse_expr(Parser *parser, Expr *expr) {
    int i = parser->token_idx;
    Token token = parser->tokens[i];

    if (token.type == TT_EOF) {
        printf("EXPR PARSER ERR: no more tokens\n");
        return false;
    }

    expr->parentheses = false;

    token = parser->tokens[i];

    if (token.type == TT_OPEN_PAREN) {
        expr->parentheses = true;
        expr->token = token;

        int saved = parser->token_idx;
        parser->token_idx = i + 1;

        Expr *lhs = parser_alloc(parser, (Expr){0});

        if (!parse_expr(parser, lhs)) {
            printf("EXPR PARSER ERR: operation without lhs(1)\n");
            parser->token_idx = saved;
            return false;
        }

        expr->lhs = lhs;
        expr->rhs = NULL;

        i = parser->token_idx;
        token = parser->tokens[i];

        if (token.type != TT_CLOSE_PAREN) {
            printf("EXPR PARSER ERR: unmatched parentheses(1)\n");
            return false;
        }

        token = parser->tokens[++i];
    }

    if (token.type == TT_EOF) {
        printf("EXPR PARSER ERR: no more tokens(2)\n");
        return false;
    }

    if (!expr->parentheses) {
        if (!tt_isoneof(token.type, 3, TT_IDENTIFIER, TT_NUMBER, TT_STRING)) {
            printf("EXPR PARSER ERR: expecting a value or identifier(1)\n");
            return false;
        }

        expr->token = token;

        token = parser->tokens[++i];
    }

    if (
        tt_isoneof(token.type, 5, TT_BOOL_OP, TT_LIKE, TT_ILIKE, TT_AND, TT_OR) ||
        tt_isoneof(token.type, 4, TT_PLUS, TT_MINUS, TT_STAR, TT_SLASH)
    ) {
        int saved = parser->token_idx;
        parser->token_idx = i + 1;
        
        Expr *rhs = parser_alloc(parser, (Expr){0});

        if (!parse_expr(parser, rhs)) {
            printf("EXPR PARSER ERR: operation without rhs(1)\n");
            parser->token_idx = saved;
            return false;
        }

        expr->lhs = parser_alloc(parser, *expr);
        expr->rhs = rhs;

        // printf("---- %s: "SVFmt"\n", getTokenTypeStr(token.type), SVArg(token.name));
        expr->token = token;
        expr->parentheses = false;

        i = parser->token_idx;
        token = parser->tokens[i];
    }

    parser->token_idx = i;

    return true;
}

bool parse_attrib_exprs(Parser *parser, AttribExpr *attr) {
    int start = parser->token_idx;
    Token token = parser->tokens[start];

    if (token.type != TT_IDENTIFIER) {
        printf("ATTRIB PARSE ERR: expecting identifier(1)\n");
        return false;
    }

    attr->base.lhs = parser_alloc(parser, (Expr){0});
    attr->base.lhs->token = token;

    token = parser->tokens[++parser->token_idx];

    if (!(token.type == TT_BOOL_OP && sv_equal(token.name, sv_make("=", 1)))) {
        printf("ATTRIB PARSE ERR: expecting '='(1)\n");
        parser->token_idx = start;
        return false;
    }

    attr->base.token = token;
    attr->base.token.type == TT_ATTRIB;

    token = parser->tokens[++parser->token_idx];

    attr->base.rhs = parser_alloc(parser, (Expr){0});

    if (!parse_expr(parser, attr->base.rhs)) {
        parser->token_idx = start;
        return false;
    }

    // printf("---- %s: "SVFmt"\n", getTokenTypeStr(token.type), SVArg(token.name));
    token = parser->tokens[parser->token_idx];

    if (token.type == TT_COMMA) {
        parser->token_idx++;
        attr->next = parser_alloc(parser, (AttribExpr){0});

        return parse_attrib_exprs(parser, attr->next);
    }

    return true;
}

bool parse_delete_stmt(Parser *parser) {
    assert(parser->tokens[parser->token_idx].type == TT_DELETE_FROM);

    DeleteStmt *stmt = parser_alloc(parser, (DeleteStmt){0});
    stmt->type = ST_DELETE;

    int start = parser->token_idx;
    Token token = parser->tokens[++parser->token_idx];

    if (token.type != TT_IDENTIFIER) {
        printf("DEL PARSER ERR: expecting identifier\n");
        return false;
    }

    stmt->table_name = token;

    token = parser->tokens[++parser->token_idx];

    if (tt_isoneof(token.type, 2, TT_EOF, TT_SEMI)) {
        if (token.type == TT_SEMI) {
            parser->token_idx++;
        }

        parser->stmt = (Stmt *)stmt;
        return true;
    }

    if (token.type != TT_WHERE) {
        printf("DEL PARSER ERR: expecting 'where' keyword\n");
        return false;
    }

    parser->token_idx++;

    stmt->where_expr = parser_alloc(parser, (Expr){0});
    // printf("stmt: %p\n", stmt);
    // printf("where_expr: %p\n", stmt->where_expr);
    // printf("st type: %d\n", stmt->type);

    if (!parse_expr(parser, stmt->where_expr)) {
        return false;
    }

    token = parser->tokens[parser->token_idx];

    // printf("---- %s: "SVFmt"\n", getTokenTypeStr(token.type), SVArg(token.name));
    if (token.type == TT_SEMI) {
        parser->token_idx++;
    }

    parser->stmt = (Stmt *)stmt;
    return true;
}

bool parse_update_stmt(Parser *parser) {
    assert(parser->tokens[parser->token_idx].type == TT_UPDATE);

    UpdateStmt *stmt = parser_alloc(parser, (UpdateStmt){0});
    stmt->type = ST_UPDATE;

    int start = parser->token_idx;
    Token token = parser->tokens[++parser->token_idx];

    if (token.type != TT_IDENTIFIER) {
        printf("UPD PARSER ERR: expecting identifier\n");
        return false;
    }

    stmt->table_name = token;

    token = parser->tokens[++parser->token_idx];

    if (token.type != TT_SET) {
        printf("UPD PARSER ERR: expecting 'set' keyword\n");
        return false;
    }

    token = parser->tokens[++parser->token_idx];

    stmt->attribs = parser_alloc(parser, (AttribExpr){0});

    if (!parse_attrib_exprs(parser, stmt->attribs)) {
        return false;
    }

    token = parser->tokens[parser->token_idx];

    if (tt_isoneof(token.type, 2, TT_EOF, TT_SEMI)) {
        if (token.type == TT_SEMI) {
            parser->token_idx++;
        }

        parser->stmt = (Stmt *)stmt;
        return true;
    }

    if (token.type != TT_WHERE) {
        printf("UPD PARSER ERR: expecting 'where' keyword\n");
        return false;
    }

    parser->token_idx++;

    stmt->where_expr = parser_alloc(parser, (Expr){0});
    // printf("stmt: %p\n", stmt);
    // printf("where_expr: %p\n", stmt->where_expr);
    // printf("st type: %d\n", stmt->type);

    if (!parse_expr(parser, stmt->where_expr)) {
        return false;
    }

    token = parser->tokens[parser->token_idx];

    // printf("---- %s: "SVFmt"\n", getTokenTypeStr(token.type), SVArg(token.name));
    if (token.type == TT_SEMI) {
        parser->token_idx++;
    }

    parser->stmt = (Stmt *)stmt;
    return true;
}

bool parse_token_list(
    Parser *parser,
    int expected_types_count,
    TokenType *expected_types,
    TokenList **out_fields
) {
    *out_fields = NULL;

    int start = parser->token_idx;
    Token token = {0};

    TokenList *fields = NULL;
    TokenList *fields_start = NULL;

    do {
        token = parser_next_token(parser);

        printf("---- %s: "SVFmt"\n", getTokenTypeStr(token.type), SVArg(token.name));
        if (!tt_isoneof_array(token.type, expected_types_count, expected_types)) {
            printf("TK LIST PARSER ERR: expecting");
            for (int i = 0; i < expected_types_count; ++i) {
                printf(" %s", getTokenTypeStr(expected_types[i]));
                if (i != expected_types_count - 1) {
                    printf(" or");
                }
            }
            printf(".\n");

            parser->token_idx = start;
            return false;
        }

        if (fields == NULL) {
            fields = parser_alloc(parser, (TokenList){0});
            fields_start = fields;
        } else {
            fields->next = parser_alloc(parser, (TokenList){0});
            fields = fields->next;
        }

        fields->value = token;

        token = parser_next_token(parser);
    } while (token.type == TT_COMMA);

    *out_fields = fields_start;

    return true;
}

bool parse_insert_stmt(Parser *parser) {
    assert(parser->tokens[parser->token_idx].type == TT_INSERT_INTO);

    InsertStmt *stmt = parser_alloc(parser, (InsertStmt){0});
    stmt->type = ST_INSERT;

    int start = parser->token_idx;

    Token token = parser_next_token(parser);

    if (token.type != TT_IDENTIFIER) {
        printf("INS PARSER ERR: expecting identifier.(1)\n");
        parser->token_idx = start;
        return false;
    }

    stmt->table_name = token;

    token = parser_next_token(parser);

    if (token.type != TT_OPEN_PAREN) {
        printf("INS PARSER ERR: expecting '('.(1)\n");
        parser->token_idx = start;
        return false;
    }
 
    TokenList *fields = NULL;

    if (!parse_token_list(parser, 1, (TokenType[]){ TT_IDENTIFIER }, &fields)) {
        parser->token_idx = start;
        return false;
    }

    stmt->fields = fields;

    token = parser_current_token(parser);

    printf("---- %s: "SVFmt"\n", getTokenTypeStr(token.type), SVArg(token.name));
    if (token.type != TT_CLOSE_PAREN) {
        printf("INS PARSER ERR: expecting ')'.(1)\n");
        parser->token_idx = start;
        return false;
    }

    token = parser_next_token(parser);

    if (token.type != TT_VALUES) {
        printf("INS PARSER ERR: expecting keyword 'values'.(1)\n");
        parser->token_idx = start;
        return false;
    }

    TokenListList *values_list = NULL;

    do {
        token = parser_next_token(parser);

        printf("---- %s: "SVFmt"\n", getTokenTypeStr(token.type), SVArg(token.name));
        if (token.type != TT_OPEN_PAREN) {
            printf("INS PARSER ERR: expecting '('.(2)\n");
            parser->token_idx = start;
            return false;
        }

        fields = NULL;

        TokenType expected[3] = { TT_NUMBER, TT_STRING, TT_IDENTIFIER };
        if (!parse_token_list(parser, 3, expected, &fields)) {
            parser->token_idx = start;
            return false;
        }

        token = parser_current_token(parser);

        if (token.type != TT_CLOSE_PAREN) {
            printf("INS PARSER ERR: expecting ')'.(2)\n");
            parser->token_idx = start;
            return false;
        }

        if (values_list == NULL) {
            values_list = parser_alloc(parser, (TokenListList){0});
            stmt->values_list = values_list;
        } else {
            values_list->next = parser_alloc(parser, (TokenListList){0});
            values_list = values_list->next;
        }

        values_list->list = fields;

        token = parser_next_token(parser);
    } while (token.type == TT_COMMA);

    if (token.type == TT_SEMI) {
        parser_next_token(parser);
    }

    parser->stmt = (Stmt *)stmt;

    return true;
}

bool parse_select_stmt(Parser *parser) {
    assert(parser->tokens[parser->token_idx].type == TT_SELECT);

    SelectStmt *stmt = parser_alloc(parser, (SelectStmt){0});
    stmt->type = ST_SELECT;

    int start = parser->token_idx;

    TokenList *fields = NULL;

    if (!parse_token_list(parser, 2, (TokenType[2]){ TT_IDENTIFIER, TT_STAR }, &fields)) {
        parser->token_idx = start;
        return false;
    }

    stmt->fields = fields;

    Token token = parser_current_token(parser);

    if (token.type != TT_FROM) {
        printf("SEL PARSER ERR: expecting keyword 'from'.(1)\n");
        parser->token_idx = start;
        return false;
    }

    token = parser_next_token(parser);

    if (token.type != TT_IDENTIFIER) {
        printf("SEL PARSER ERR: expecting identifier.(2)\n");
        parser->token_idx = start;
        return false;
    }

    stmt->table_name = token;

    token = parser_next_token(parser);

    if (token.type == TT_GROUP_BY) {
        stmt->group_by = parser_alloc(parser, (GroupByDecl){0});

        if (!parse_token_list(parser, 1, (TokenType[1]){ TT_IDENTIFIER }, &fields)) {
            parser->token_idx = start;
            return false;
        }

        stmt->group_by->fields =  fields;

        if (token.type == TT_HAVING) {
            token = parser_next_token(parser);
            
            stmt->group_by->having_expr_list = parser_alloc(parser, (Expr){0});

            if (!parse_expr(parser, stmt->group_by->having_expr_list)) {
                parser->token_idx = start;
                return false;
            }
        }
    }

    if (token.type == TT_SEMI) {
        parser_next_token(parser);
    }

    parser->stmt = (Stmt *)stmt;

    return true;
}

Token *tk_read_tokens(const char *text) {
    Tokenizer tokenizer = tk_make(text);
    Token *tokens = darray_create(Token);
    Token token = {0};

    do {
        if (!tk_next_token(&tokenizer, &token)) {
            printf("ERR: %s\n", tokenizer.err_msg);
            return false;
        }

        darray_push(tokens, token);
        // const char *tk_name = getTokenTypeStr(token.type);
        // printf("%s: %.*s\n", tk_name, token.name.size, token.name.data);
    } while (token.type != TT_EOF);

    return tokens;
}

bool parse(Parser *parser) {
    int token_count = darray_length(parser->tokens);
    assert(token_count >= 1);

    Token token = parser->tokens[0];

    switch (token.type) {
        case TT_DELETE_FROM:
            return parse_delete_stmt(parser);
        case TT_UPDATE:
            return parse_update_stmt(parser);
        case TT_INSERT_INTO:
            return parse_insert_stmt(parser);
        case TT_SELECT:
            return parse_select_stmt(parser);
    }

    return false;
}

void print_expr(Expr *expr, int depth) {
    if (expr == NULL) {
        return;
    }

    printf("%*s", depth, "");

    if (expr->parentheses) {
        printf("()\n");
    } else {
        printf(SVFmt"\n", SVArg(expr->token.name));
    }

    print_expr(expr->lhs, depth + 2);
    print_expr(expr->rhs, depth + 2);
}

void print_attrib_exprs(AttribExpr *attr, int depth) {
    if (attr == NULL) {
        return;
    }

    printf("%*s=\n", depth, "");
    print_expr(attr->base.lhs, depth + 2);
    print_expr(attr->base.rhs, depth + 2);
    print_attrib_exprs(attr->next, depth);
}

void print_delete_stmt(DeleteStmt *stmt) {
    assert(stmt->type == ST_DELETE);

    printf("DELETE FROM\n");
    printf("table name = "SVFmt"\n", SVArg(stmt->table_name.name));
    print_expr(stmt->where_expr, 0);
}

void print_update_stmt(UpdateStmt *stmt) {
    assert(stmt->type == ST_UPDATE);

    printf("UPDATE\n");
    printf("table name = "SVFmt"\n", SVArg(stmt->table_name.name));
    printf("attribs:\n");
    print_attrib_exprs(stmt->attribs, 0);
    printf("where:\n");
    print_expr(stmt->where_expr, 0);
}

void print_insert_stmt(InsertStmt *stmt) {
    assert(stmt->type == ST_INSERT);

    printf("INSERT INTO\n");
    printf("table name = "SVFmt"\n", SVArg(stmt->table_name.name));
    printf("fields:\n");

    TokenList *fields = stmt->fields;
    while (fields) {
        printf("  "SVFmt"\n", SVArg(fields->value.name));
        fields = fields->next;
    }

    printf("values:\n");
    TokenListList *values_list = stmt->values_list;

    while (values_list) {
        fields = values_list->list;
        printf("  ");

        while (fields) {
            printf(SVFmt, SVArg(fields->value.name));
            fields = fields->next;

            if (fields) {
                printf(", ");
            } else {
                printf("\n");
            }
        }

        values_list = values_list->next;
    }
}

void print_select_stmt(SelectStmt *stmt) {
    assert(stmt->type == ST_SELECT);

    printf("SELECT FROM\n");
    printf("table name = "SVFmt"\n", SVArg(stmt->table_name.name));
    printf("fields:\n");

    TokenList *fields = stmt->fields;
    while (fields) {
        printf("  "SVFmt"\n", SVArg(fields->value.name));
        fields = fields->next;
    }

    if (stmt->group_by) {
        printf("group by:\n");

        fields = stmt->group_by->fields;
        while (fields) {
            printf("  "SVFmt"\n", SVArg(fields->value.name));
            fields = fields->next;
        }

        printf("having:\n");
        print_expr(stmt->group_by->having_expr_list, 0);
    }
}

void print_stmt(Stmt *stmt) {
    switch (stmt->type) {
        case ST_DELETE:
            print_delete_stmt((DeleteStmt *)stmt);
            break;
        case ST_UPDATE:
            print_update_stmt((UpdateStmt *)stmt);
            break;
        case ST_INSERT:
            print_insert_stmt((InsertStmt *)stmt);
            break;
        case ST_SELECT:
            print_select_stmt((SelectStmt *)stmt);
            break;
        default:
            printf("st type: %d\n", stmt->type);
            break;
    }
}

static const char *test =
    "select *, x, y from restaurants group by x;";

static const char *test5 =
    "insert into restaurants (a, b, c) values (1, 2, 3), (4, 5, 6);";

static const char *test4 =
    "update restaurants set v = x + 1, p = 8 where y > 3;";

static const char *test3 =
    "delete from restaurants where ((x) + 1);";

static const char *test2 =
    "select \"rst\".\"88\", a, b, c.*, 123, 5.6 from x\n"
    "inner join y on x.id <> y.id\n"
    "left join y on x.id = y.id\n"
    "right outer join y on x.id = y.id\n"
    "group by x.id\n"
    "order by x.id"
    ;

int main() {
    Parser p = parser_make(tk_read_tokens(test));

    if (!parse(&p)) {
        printf("parse fail\n");
    } else {
        print_stmt(p.stmt);
    }

    parser_clear(&p);

    return 0;
}

StringView sv_make(const char *data, int size) {
    return (StringView) { data, size };
}

StringView sv_make2(const char *data) {
    return (StringView) { .data = data, .size = strlen(data) };
}

long sv_offset(StringView v1, StringView v2) {
    long off = (long)(v2.data - v1.data);

    return off > 0 ? off : 0;
}

bool sv_equal(StringView v1, StringView v2) {
    if (v1.size != v2.size) {
        return false;
    }

    for (int i = 0; i < v1.size; ++i) {
        if (v1.data[i] != v2.data[i]) {
            return false;
        }
    }

    return true;
}

#define LOWER(c) ('A' <= (c) && (c) <= 'Z') ? ((c) | 0x20) : c

bool sv_equal_i(StringView v1, StringView v2) {
    if (v1.size != v2.size) {
        return false;
    }

    for (int i = 0; i < v1.size; ++i) {
        char c1 = LOWER(v1.data[i]);
        char c2 = LOWER(v2.data[i]);

        if (c1 != c2) {
            return false;
        }
    }

    return true;
}

StringView sv_chop_str(StringView *str, int amount) {
    if (amount > str->size) {
        amount = str->size;
    }

    StringView view;
    view.data = str->data;
    view.size = amount;

    str->size -= amount;
    str->data += amount;

    return view;
}

StringView sv_skip(StringView str, int amount) {
    if (amount > str.size) {
        amount = str.size;
    }

    return (StringView) {
        .size = str.size - amount,
        .data = str.data + amount,
    };
}

StringView sv_skip_spaces(StringView str) {
    int n = 0;
    while (n < str.size) {
        if (IS_SPACE(str.data[n])) {
            ++n;
        } else {
            break;
        }
    }

    return (StringView) {
        .size = str.size - n,
        .data = str.data + n,
    };
}

bool sv_is_empty(StringView str) {
    return str.size == 0;
}

StringView sv_read_until_char(StringView str, char chr) {
    if (str.size == 0) {
        return (StringView){0};
    }

    int n = 0;
    char c = str.data[n];
    while (c != chr) {
        n += 1;

        if (n >= str.size) {
            break;
        }

        c = str.data[n];
    }

    return (StringView) {
        .data = str.data,
        .size = n,
    };
}

StringView sv_read_while_alnum(StringView str) {
    if (str.size == 0) {
        return (StringView){0};
    }

    int n = 0;
    char c = str.data[n];
    while (IS_ALNUM(c)) {
        n += 1;

        if (n >= str.size) {
            break;
        }

        c = str.data[n];
    }

    return (StringView) {
        .data = str.data,
        .size = n,
    };
}

StringView sv_read_while_alpha(StringView str) {
    if (str.size == 0) {
        return (StringView){0};
    }

    int n = 0;
    char c = str.data[n];
    while (IS_ALPHA(c)) {
        n += 1;

        if (n >= str.size) {
            break;
        }

        c = str.data[n];
    }

    return (StringView) {
        .data = str.data,
        .size = n,
    };
}

StringView sv_read_while_number(StringView str) {
    if (str.size == 0) {
        return (StringView){0};
    }

    int n = 0;
    char c = str.data[n];
    while (IS_DIGIT(c)) {
        n += 1;

        if (n >= str.size) {
            break;
        }

        c = str.data[n];
    }

    return (StringView) {
        .data = str.data,
        .size = n,
    };
}

StringView sv_read_file(const char *filename) {
    FILE *fp = fopen(filename, "r");

    if (fp == NULL) {
        printf("Could not open file: %s\n", filename);

        return (StringView){0};
    }

    if (fseek(fp, 0, SEEK_END)) {
        printf("Could not find the file end!\n");
        printf("::: %s\n", strerror(errno));
        fclose(fp);

        return (StringView){0};
    }

    int file_size = ftell(fp);

    if (fseek(fp, 0, SEEK_SET)) {
        printf("Could not find the file start!\n");
        printf("::: %s\n", strerror(errno));
        fclose(fp);

        return (StringView){0};
    }

    char *file_data = malloc((file_size + 1) * sizeof(char));
    
    if (fread(file_data, file_size, 1, fp) != 1) {
        printf("Could not read the file content!\n");
        printf("::: %s\n", strerror(errno));
        fclose(fp);

        return (StringView){0};
    }

    file_data[file_size] = '\0';

    fclose(fp);

    return (StringView){ .data = file_data, .size = file_size };
}

// Tokenizer

Tokenizer tk_make(const char *text) {
    Tokenizer tokenizer = {0};
    tokenizer.data = sv_make2(text);
    tokenizer.unchanged = tokenizer.data;

    return tokenizer;
}

Token tk_make_identifier_token(Tokenizer *tokenizer, StringView data) {
    int size = sv_offset(tokenizer->data, data);
    StringView name = sv_make(tokenizer->data.data, size);

    tokenizer->data = sv_skip(tokenizer->data, size);

    return (Token) {
        .type = TT_IDENTIFIER,
        .name = name,
    };
}

#define RETURN_TK_ERROR(t, msg, ...) \
    snprintf((t)->err_msg, 128, msg, __VA_ARGS__); \
    return false

bool tk_read_identifier(Tokenizer *tokenizer, Token *token) {
    assert(!sv_is_empty(tokenizer->data));

    StringView data = tokenizer->data;

    char c = data.data[0];
    if (c == '"') {
        data = sv_skip(data, 1);
        StringView s = sv_read_until_char(data, '"');
        data = sv_skip(data, s.size + 1);
    } else if (IS_ALPHA(c)) {
        StringView s = sv_read_while_alnum(data);
        data = sv_skip(data, s.size);
    } else {
        int size = MIN(10, tokenizer->data.size);

        RETURN_TK_ERROR(
            tokenizer,
            "Could not read identifier at: %.*s",
            size,
            tokenizer->data.data
        );
    }

    StringView saved = data;

    data = sv_skip_spaces(data);

    if (sv_is_empty(data)) {
        data = saved;
        *token = tk_make_identifier_token(tokenizer, data);
        return true;
    }

    c = data.data[0];

    if (c == '.') {
        data = sv_skip(data, 1);
        data = sv_skip_spaces(data);

        if (sv_is_empty(data)) {
            int size = MIN(10, tokenizer->data.size);

            RETURN_TK_ERROR(
                tokenizer,
                "Could not read identifier at: %.*s",
                size,
                tokenizer->data.data
            );
        }

        c = data.data[0];
        if (c == '"') {
            data = sv_skip(data, 1);
            StringView s = sv_read_until_char(data, '"');
            data = sv_skip(data, s.size + 1);

            *token = tk_make_identifier_token(tokenizer, data);
            return true;
        }

        if (c == '*') {
            data = sv_skip(data, 1);

            *token = tk_make_identifier_token(tokenizer, data);
            return true;
        }

        if (IS_ALPHA(c)) {
            StringView s = sv_read_while_alnum(data);
            data = sv_skip(data, s.size);

            *token = tk_make_identifier_token(tokenizer, data);
            return true;
        }

        int size = MIN(10, tokenizer->data.size);
        RETURN_TK_ERROR(
            tokenizer,
            "Could not read identifier at: %.*s",
            size,
            tokenizer->data.data
        );
    } else {
        data = saved;
    }

    *token = tk_make_identifier_token(tokenizer, data);
    return true;
}

bool tk_read_number(Tokenizer *tokenizer, Token *token) {
    assert(!sv_is_empty(tokenizer->data));

    StringView data = tokenizer->data;

    char c = data.data[0];
    if (c == '-') {
        data = sv_skip(data, 1);
        data = sv_skip_spaces(data);
    }

    bool has_dot = false;
    if (c == '.') {
        data = sv_skip(data, 1);
        has_dot = true;
    }

    if (IS_DIGIT(c)) {
        StringView s = sv_read_while_number(data);
        data = sv_skip(data, s.size);

        c = data.data[0];
        if (!has_dot && !sv_is_empty(data) && c == '.') {
            data = sv_skip(data, 1);

            s = sv_read_while_number(data);
            data = sv_skip(data, s.size);
        }

        int size = sv_offset(tokenizer->data, data);
        StringView name = sv_make(tokenizer->data.data, size);

        tokenizer->data = sv_skip(tokenizer->data, size);

        token->type = TT_NUMBER;
        token->name = name;

        return true;
    }

    int size = MIN(10, tokenizer->data.size);
    RETURN_TK_ERROR(
        tokenizer,
        "Could not read number at: %.*s",
        size,
        tokenizer->data.data
    );
}

bool tk_read_string(Tokenizer *tokenizer, Token *token) {
    assert(!sv_is_empty(tokenizer->data));

    StringView data = tokenizer->data;

    if (data.data[0] == '\'') {
        // TODO: Return error if it contains new lines
        data = sv_skip(data, 1);
        data = sv_read_until_char(data, '\'');
        data = sv_skip(data, 1);

        int size = sv_offset(tokenizer->data, data);
        StringView name = sv_make(tokenizer->data.data, size);

        tokenizer->data = sv_skip(tokenizer->data, size);

        token->type = TT_NUMBER;
        token->name = name;
    }

    int size = MIN(10, tokenizer->data.size);
    RETURN_TK_ERROR(
        tokenizer,
        "Could not read string at: %.*s",
        size,
        tokenizer->data.data
    );
}

bool tk_next_token(Tokenizer *tokenizer, Token *token) {
    tokenizer->data = sv_skip_spaces(tokenizer->data);

    if (sv_is_empty(tokenizer->data)) {
        token->type = TT_EOF;
        token->name = sv_make(NULL, 0);
        return true;
    }

    char c = tokenizer->data.data[0];

    switch (c) {
        case '-':
            token->type = TT_MINUS;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            return true;
        case '+':
            token->type = TT_PLUS;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            return true;
        case '/':
            token->type = TT_SLASH;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            return true;
        case '*':
            token->type = TT_STAR;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            return true;
        case '>':
            token->type = TT_BOOL_OP;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);

            if (!sv_is_empty(tokenizer->data)) {
                c = tokenizer->data.data[0];

                if (c == '=') {
                    token->name.size += 1;
                    tokenizer->data = sv_skip(tokenizer->data, 1);
                }
            }
            return true;
        case '<':
            token->type = TT_BOOL_OP;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);

            if (!sv_is_empty(tokenizer->data)) {
                c = tokenizer->data.data[0];

                if (c == '=' || c == '>') {
                    token->name.size += 1;
                    tokenizer->data = sv_skip(tokenizer->data, 1);
                }
            }
            return true;
        case '=':
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            token->type = TT_BOOL_OP;
            return true;
        case '!':
            if (tokenizer->data.size >= 2) {
                StringView s = sv_make(tokenizer->data.data, 2);

                if (sv_equal(s, sv_make("!=", 2))) {
                    token->type = TT_BOOL_OP;
                    token->name = sv_make(tokenizer->data.data, 2);
                    tokenizer->data = sv_skip(tokenizer->data, 2);
                    return true;
                }
            }

            RETURN_TK_ERROR(
                tokenizer,
                "Could not read symbol at: %.*s",
                MIN(10, tokenizer->data.size),
                tokenizer->data.data
            );
        case '(':
            token->type = TT_OPEN_PAREN;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            return true;
        case ')':
            token->type = TT_CLOSE_PAREN;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            return true;
        case ',':
            token->type = TT_COMMA;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            return true;
        case ';':
            token->type = TT_SEMI;
            token->name = sv_make(tokenizer->data.data, 1);
            tokenizer->data = sv_skip(tokenizer->data, 1);
            return true;
        case '\'':
            return tk_read_string(tokenizer, token);
        default:
            if (IS_DIGIT(c) || c == '.') {
                return tk_read_number(tokenizer, token);
            }

            if (c == '"' || IS_ALPHA(c)) {
                if (tk_read_identifier(tokenizer, token)) {
                    for (int i = 0; i < TT_TOKEN_COUNT; ++i) {
                        const char **keywords = keywords_map[i];

                        if (*keywords != NULL) {
                            StringView s = sv_make2(*keywords);

                            if (sv_equal_i(s, token->name)) {
                                bool found = true;
                                StringView saved_point = tokenizer->data;
                                Token saved_token = *token;
                                
                                keywords += 1;
                                while (*keywords != NULL) {
                                    s = sv_make2(*keywords);

                                    tokenizer->data = sv_skip_spaces(tokenizer->data);

                                    if (sv_is_empty(tokenizer->data)) {
                                        found = false;
                                        break;
                                    }

                                    if (!tk_read_identifier(tokenizer, token)) {
                                        found = false;
                                        break;
                                    }

                                    if (!sv_equal_i(s, token->name)) {
                                        found = false;
                                        break;
                                    }

                                    keywords += 1;
                                }

                                if (!found) {
                                    *token = saved_token;
                                    tokenizer->data = saved_point;
                                } else {
                                    token->type = i;
                                    int size = sv_offset(saved_token.name, token->name) + token->name.size;
                                    token->name = sv_make(saved_token.name.data, size);
                                    break;
                                }
                            }
                        }
                    }

                    return true;
                } else {
                    return false;
                }
            }

            break;
    }

    RETURN_TK_ERROR(
        tokenizer,
        "Could not read symbol at: %.*s",
        MIN(10, tokenizer->data.size),
        tokenizer->data.data
    );
}
