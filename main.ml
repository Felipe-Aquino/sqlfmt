(* TODO: Implement the formating *)

(* List *)
let drop (n: int) (ls: 'a list): 'a list =
  let rec loop k l =
    if k > 0 then
      match l with
      | [] -> []
      | hd :: tl -> loop (k - 1) tl
    else
      l
  in
    loop n ls

let join (delimiter: string) (f: 'a -> string) (ls: 'a list): string =
  let rec loop l result =
    match l with
    | [] -> result
    | hd :: tl ->
      let result =
        if String.length result > 0 then
          result ^ delimiter ^ (f hd)
        else
          (f hd)
      in
      loop tl result
  in
    loop ls ""

(* Tokenizing *)

type token_type_t =
  | Eof
  | Identifier of string
  | Number of string
  | String of string
  | Attrib
  | Plus
  | Minus
  | Slash
  | Star
  | Gt | Ge | Lt | Le | Equal
  | And
  | Or
  | Like
  | ILike
  | In
  | NotIn
  | OpenParen
  | CloseParen
  | Semi
  | Comma
  | Select
  | InsertInto
  | DeleteFrom
  | Update
  | From
  | Set
  | Values
  | InnerJoin
  | LeftJoin
  | RightJoin
  | LeftOuterJoin
  | RightOuterJoin
  | On
  | Where
  | IsNot
  | Is
  | OrderBy
  | GroupBy
  | Having
  | Asc
  | Desc
  | Limit
  | Offset
  | As
  | Distinct

type token_t =
  { ttype: token_type_t
  ; line: int
  ; column: int
  ; pos: int
  }

let int_of_token_type = function
  | Eof -> 0
  | Identifier _ -> 1
  | Number _ -> 2
  | String _ -> 3
  | Attrib -> 4
  | Plus -> 5
  | Minus -> 6
  | Slash -> 7
  | Star -> 8
  | Gt -> 9
  | Ge -> 10
  | Lt -> 11
  | Le -> 12
  | Equal -> 13
  | And -> 14
  | Or -> 15
  | Like -> 15
  | ILike -> 16
  | In -> 17
  | NotIn -> 18
  | OpenParen -> 19
  | CloseParen -> 20
  | Semi -> 21
  | Comma -> 22
  | Select -> 24
  | InsertInto -> 25
  | DeleteFrom -> 26
  | Update -> 27
  | From -> 28
  | Set -> 29
  | Values -> 30
  | InnerJoin -> 31
  | LeftJoin -> 32
  | RightJoin -> 33
  | LeftOuterJoin -> 34
  | RightOuterJoin -> 35
  | On -> 36
  | Where -> 37
  | IsNot -> 38
  | Is -> 39
  | OrderBy -> 40
  | GroupBy -> 41
  | Having -> 42
  | Asc -> 43
  | Desc -> 44
  | Limit -> 45
  | Offset -> 46
  | As -> 47
  | Distinct -> 48

let sprint_token_type = function
  | Eof -> "Eof"
  | Identifier v -> Printf.sprintf "Identifier(%s)" v
  | Number v -> Printf.sprintf "Number(%s)" v
  | String v -> Printf.sprintf "String(%s)" v
  | Attrib -> "Attrib"
  | Plus -> "Plus"
  | Minus -> "Minus"
  | Slash -> "Slash"
  | Star -> "Star"
  | Gt -> "Gt"
  | Ge -> "Ge"
  | Lt -> "Lt"
  | Le -> "Le"
  | Equal -> "Equal"
  | And -> "And"
  | Or -> "Or"
  | Like -> "Like"
  | ILike -> "ILike"
  | In -> "In"
  | NotIn -> "NotIn"
  | OpenParen -> "OpenParen"
  | CloseParen -> "CloseParen"
  | Semi -> "Semi"
  | Comma -> "Comma"
  | Select -> "Select"
  | InsertInto -> "InsertInto"
  | DeleteFrom -> "DeleteFrom"
  | Update -> "Update"
  | From -> "From"
  | Set -> "Set"
  | Values -> "Values"
  | InnerJoin -> "InnerJoin"
  | LeftJoin -> "LeftJoin"
  | RightJoin -> "RightJoin"
  | LeftOuterJoin -> "LeftOuterJoin"
  | RightOuterJoin -> "RightOuterJoin"
  | On -> "On"
  | Where -> "Where"
  | IsNot -> "IsNot"
  | Is -> "Is"
  | OrderBy -> "OrderBy"
  | GroupBy -> "GroupBy"
  | Having -> "Having"
  | Asc -> "Asc"
  | Desc -> "Desc"
  | Limit -> "Limit"
  | Offset -> "Offset"
  | As -> "As"
  | Distinct -> "Distinct"

let sprint_token_name_or_symbol = function
  | Eof -> "EOF"
  | Identifier v -> v
  | Number v -> v
  | String v -> v
  | Attrib -> "="
  | Plus -> "+"
  | Minus -> "-"
  | Slash -> "/"
  | Star -> "*"
  | Gt -> ">"
  | Ge -> ">="
  | Lt -> "<"
  | Le -> "<="
  | Equal -> "="
  | And -> "AND"
  | Or -> "OR"
  | Like -> "LIKE"
  | ILike -> "ILIKE"
  | In -> "IN"
  | NotIn -> "NOTIN"
  | OpenParen -> "("
  | CloseParen -> ")"
  | Semi -> ";"
  | Comma -> ","
  | Select -> "SELECT"
  | InsertInto -> "INSERT into"
  | DeleteFrom -> "DELETE FROM"
  | Update -> "UPDATE"
  | From -> "FROM"
  | Set -> "SET"
  | Values -> "VALUES"
  | InnerJoin -> "INNER JOIN"
  | LeftJoin -> "LEFT JOIN"
  | RightJoin -> "RIGHT JOIN"
  | LeftOuterJoin -> "LEFT OUTER JOIN"
  | RightOuterJoin -> "RIGHT OUTER JOIN"
  | On -> "ON"
  | Where -> "WHERE"
  | IsNot -> "IS NOT"
  | Is -> "IS"
  | OrderBy -> "ORDER BY"
  | GroupBy -> "GROUP BY"
  | Having -> "HAVING"
  | Asc -> "ASC"
  | Desc -> "DESC"
  | Limit -> "LIMIT"
  | Offset -> "OFFSET"
  | As -> "AS"
  | Distinct -> "DISTINCT"

let keywords_map: (token_type_t * string list) list =
  [ (And,            ["and"])
  ; (Or,             ["or"])
  ; (Like,           ["like"])
  ; (ILike,          ["ilike"])
  ; (In,             ["in"])
  ; (NotIn,          ["not"; "in"])
  ; (Select,         ["select"])
  ; (InnerJoin,      ["inner"; "join"])
  ; (LeftJoin,       ["left"; "join"])
  ; (RightJoin,      ["right"; "join"])
  ; (LeftOuterJoin,  ["left"; "outer"; "join"])
  ; (RightOuterJoin, ["right"; "outer"; "join"])
  ; (On,             ["on"])
  ; (Where,          ["where"])
  ; (IsNot,          ["is"; "not"])
  ; (Is,             ["is"])
  ; (OrderBy,        ["order"; "by"])
  ; (GroupBy,        ["group"; "by"])
  ; (Having,         ["having"])
  ; (Asc,            ["asc"])
  ; (Desc,           ["desc"])
  ; (Limit,          ["limit"])
  ; (Offset,         ["offset"])
  ; (InsertInto,     ["insert"; "into"])
  ; (DeleteFrom,     ["delete"; "from"])
  ; (Values,         ["values"])
  ; (Update,         ["update"])
  ; (Set,            ["set"])
  ; (From,           ["from"])
  ; (As,             ["as"])
  ; (Distinct,       ["distinct"])
  ]

type context_t =
  { data: string
  ; pos: int
  ; start: int
  ; line: int
  ; column: int
  }

let make_token (ctx: context_t) (tt: token_type_t) =
  { ttype = tt
  ; pos = ctx.pos
  ; column = ctx.column
  ; line = ctx.line
  }

let print_token token =
  let name = sprint_token_type token.ttype in
    Printf.printf "  %d:%d(%d) %s\n" token.line token.column token.pos name

let sprint_token token =
  let name = sprint_token_type token.ttype in
    Printf.sprintf "%s at %d:%d(%d)" name token.line token.column token.pos

let token_type_cmp (tt1: token_type_t) (tt2: token_type_t): bool =
  (int_of_token_type tt1) == (int_of_token_type tt2)

let fmt_ctx_pos ctx =
    Printf.sprintf "%d:%d(%d)" ctx.line ctx.column ctx.pos

let is_space (c: char): bool =
  c == ' ' || c == '\t' || c == '\n' || c == '\r'

let is_alpha (c: char): bool =
  (c >= 'a' && c <= 'z') ||
  (c >= 'A' && c <= 'Z') ||
  c == '_'

let is_digit (c: char): bool = c >= '0' && c <= '9'

let is_alnum (c: char): bool = is_alpha c || is_digit c || c == '_'

let is_eof (ctx: context_t): bool =
  String.length ctx.data <= ctx.pos

let get_next_char (ctx: context_t): char option =
  if not (is_eof ctx) then
    Some (String.get ctx.data (ctx.pos + 1))
  else
    None

let get_current_char (ctx: context_t): char = String.get ctx.data ctx.pos

let get_current_char_opt (ctx: context_t): char option =
  if not (is_eof ctx) then
    Some (String.get ctx.data ctx.pos)
  else
    None

let current_substr (ctx: context_t): string =
  String.sub ctx.data ctx.start (ctx.pos - ctx.start)

let advance (ctx: context_t): context_t =
  if not (is_eof ctx) then
  begin
    let ctx' = { ctx with pos = ctx.pos + 1 } in
    if not (is_eof ctx') then
      if String.get ctx'.data ctx'.pos == '\n' then
        { ctx' with column = 1
        ; line = ctx'.line + 1
        }
      else
        { ctx' with column = ctx'.column + 1 }
    else
      ctx'
  end
  else
    ctx

let advance_n (ctx: context_t) (n: int): context_t =
  let rec loop ctx' k =
    if k < n then
      loop (advance ctx') (k + 1)
    else
      ctx'
  in
    loop ctx 0

let advance_while (ctx: context_t) (f: char -> bool): context_t =
  let rec loop ctx' =
    match get_current_char_opt ctx' with
    | Some c when f c -> loop (advance ctx')
    | _ -> ctx'
  in
    loop ctx

let rec skip_spaces (ctx: context_t): context_t =
  match get_current_char_opt ctx with
  | Some c when is_space c -> skip_spaces (advance ctx)
  | _ -> ctx

let read_number (ctx: context_t): (context_t * token_t, string) result =
  let ctx' = { ctx with start = ctx.pos } in
    match get_current_char ctx' with
    | c when c == '.' -> begin
      match get_next_char ctx' with
      | Some c when is_digit c ->
        let ctx' = advance_while (advance ctx') is_digit in
        let value = current_substr ctx' in
        let token = make_token ctx (Number value) in
          Ok (ctx', token)
      | _ ->
        Error (Printf.sprintf "Expecting number at %s" (fmt_ctx_pos ctx))
    end

    | c when is_digit c -> begin
      let ctx' = advance_while ctx' is_digit in
        match get_current_char_opt ctx' with
        | Some c when c == '.' ->
          let ctx' = advance_while (advance ctx') is_digit in
          let value = current_substr ctx' in
          let token = make_token ctx (Number value) in
            Ok (ctx', token)

        | _ ->
          let value = current_substr ctx' in
          let token = make_token ctx (Number value) in
            Ok (ctx', token)
    end
    | _ ->
      Error (Printf.sprintf "Expecting number at %s" (fmt_ctx_pos ctx))

let read_string (ctx: context_t): (context_t * token_t, string) result =
  match get_current_char_opt ctx with
  | Some '\'' -> (
    let ctx' = advance { ctx with start = ctx.pos } in
    let ctx' = advance_while ctx' (fun c -> c != '\'') in
    match get_current_char_opt ctx' with
    | Some '\'' ->
      let ctx' = advance ctx' in
      let value = current_substr ctx' in
      let token = make_token ctx (String value) in
        Ok (ctx', token)
    | _ ->
      Error (Printf.sprintf "Expecting string at %s" (fmt_ctx_pos ctx))
  )
  | _ ->
    Error (Printf.sprintf "Expecting string at %s" (fmt_ctx_pos ctx))

let read_simple_ident (ctx: context_t): (context_t * string, unit) result =
  let ctx' = { ctx with start = ctx.pos } in
    match get_current_char_opt ctx with
    | Some '"' -> begin
      let ctx' = advance ctx' in
      let ctx' = advance_while ctx' (fun c -> c != '"') in
        match get_current_char_opt ctx' with
        | Some '"' ->
          let ctx' = advance ctx' in
          let value = current_substr ctx' in
            Ok (ctx', value)
        | _ -> Error ()
    end
    | Some c when c == '_' || is_alpha c ->
      let ctx' = advance_while ctx' is_alnum in
      let value = current_substr ctx' in
        Ok (ctx', value)
    | _ ->
      Error ()

let read_ident (ctx: context_t): (context_t * token_t, string) result =
  match read_simple_ident ctx with
  | Ok (ctx', value1) -> begin
    let ctx2 = skip_spaces ctx' in
    match get_current_char_opt ctx2 with
    | Some '.' -> (
      let ctx2 = advance ctx2 in
      match read_simple_ident ctx2 with
      | Ok (ctx2, value2) ->
        let token = make_token ctx (Identifier (value1 ^ "." ^ value2)) in
          Ok (ctx2, token)
      | Error _ ->
        match get_current_char_opt ctx2 with
        | Some '*' -> 
          let ctx2 = advance ctx2 in
          let token = make_token ctx (Identifier (value1 ^ ".*")) in
            Ok (ctx2, token)
        | _ ->
          Error (Printf.sprintf "Expecting identifier at %s" (fmt_ctx_pos ctx))
    )
    | _ ->
      let token = make_token ctx (Identifier value1) in
        Ok (ctx', token)
  end
  | Error _ ->
    Error (Printf.sprintf "Expecting identifier at %s" (fmt_ctx_pos ctx))

let read_token (ctx: context_t): (context_t * token_t, string) result =
  let ctx = skip_spaces ctx in
    match get_current_char_opt ctx with
    | None -> Ok (advance ctx, make_token ctx Eof)
    | Some c when is_digit c || c == '.' -> read_number ctx
    | Some c when is_alpha c || c == '"' -> read_ident ctx
    | Some '\'' -> read_string ctx
    | Some '*' -> Ok (advance ctx, make_token ctx Star)
    | Some '+' -> Ok (advance ctx, make_token ctx Plus)
    | Some '-' -> Ok (advance ctx, make_token ctx Minus)
    | Some '/' -> Ok (advance ctx, make_token ctx Slash)
    | Some ',' -> Ok (advance ctx, make_token ctx Comma)
    | Some ';' -> Ok (advance ctx, make_token ctx Semi)
    | Some '=' -> Ok (advance ctx, make_token ctx Equal)
    | Some '(' -> Ok (advance ctx, make_token ctx OpenParen)
    | Some ')' -> Ok (advance ctx, make_token ctx CloseParen)
    | Some '>' -> begin
      match get_next_char ctx with
      | Some '=' -> Ok (advance_n ctx 2, make_token ctx Ge)
      | _ -> Ok (advance ctx, make_token ctx Gt)
    end
    | Some '<' -> begin
      match get_next_char ctx with
      | Some '=' -> Ok (advance_n ctx 2, make_token ctx Le)
      | _ -> Ok (advance ctx, make_token ctx Lt)
    end
    | Some '!' -> begin
      match get_next_char ctx with
      | Some '=' -> Ok (advance_n ctx 2, make_token ctx Equal)
      | _ ->
        Error (Printf.sprintf "Expecting symbol '=' after %s" (fmt_ctx_pos ctx))
    end
    | Some v ->
      Printf.printf "-- '%c' at %s" v (fmt_ctx_pos ctx);
      Error "Unknown token type"

let replace_keywords (tokens: token_t list): token_t list =
  let rec zip_lhs f xs ys =
    if List.is_empty xs then
      true
    else
      let x = List.hd xs in
      let y = List.hd ys in
      if f x y then
        zip_lhs f (List.tl xs) (List.tl ys)
      else
        false
  in
  let test_keyword (_, names) tokens' =
    zip_lhs
      (fun name token ->
        match token.ttype with
        | Identifier v -> v |> String.lowercase_ascii |> (String.equal name)
        | _ -> false
      )
      names
      tokens'
  in
  let rec loop tokens' result =
    match tokens' with
    | [] -> result
    | hd :: remaining -> begin
      let item_opt =
        List.find_opt
          (fun keyword_item -> test_keyword keyword_item tokens')
          keywords_map
      in
        match item_opt with
        | Some (token_type, names) ->
          let count = List.length names in
          let new_token =
            { ttype = token_type
            ; pos = hd.pos
            ; column = hd.column
            ; line = hd.line
            }
          in
            loop (drop count tokens') (new_token :: result)
        | None ->
          loop remaining (hd :: result)
    end
  in
    List.rev (loop tokens [])


let tokenize (ctx: context_t): (token_t list, string) result =
  let rec loop ctx' tokens =
    match read_token ctx' with
    | Ok (ctx', token) ->
      if token_type_cmp token.ttype Eof then
        Ok (replace_keywords (List.rev tokens))
      else
        loop ctx' (token :: tokens)
    | Error e -> Error e
  in
    loop ctx []

(* Parsing *)

let ( let* ) = Result.bind

type parser_t =
  { tokens: token_t array
  ; token_count: int
  ; pos: int
  }

type 'a parser_result_t = (parser_t * 'a, string) result

type join_type_t =
  | InnerJoin
  | LeftJoin
  | RightJoin
  | LeftOuterJoin
  | RightOuterJoin

let to_join_type (tt: token_type_t): join_type_t =
  match tt with
  | InnerJoin -> InnerJoin 
  | LeftJoin -> LeftJoin 
  | RightJoin -> RightJoin 
  | LeftOuterJoin -> LeftOuterJoin 
  | RightOuterJoin -> RightOuterJoin 
  | _ -> failwith "could not transform join token type"

let from_join_type (jt: join_type_t): token_type_t =
  match jt with
  | InnerJoin -> InnerJoin 
  | LeftJoin -> LeftJoin 
  | RightJoin -> RightJoin 
  | LeftOuterJoin -> LeftOuterJoin 
  | RightOuterJoin -> RightOuterJoin 

type expr_t =
  | Value of token_type_t
  | Parentheses of expr_t
  | BinOp of { op: token_type_t; lhs: expr_t; rhs: expr_t }
  | InList of { name: expr_t; op: token_type_t; values: expr_t list }

type assign_t =
  { lhs: token_type_t
  ; rhs: expr_t
  }

type field_t =
  { expr: expr_t
  ; alias: string option
  }

type where_stmt_t =
  { expr: expr_t
  }

type single_order_by_item_t =
  { expr: expr_t
  ; ascending: bool 
  }

type order_by_stmt_t =
  { items: single_order_by_item_t list }

type group_by_stmt_t =
  { names: string list
  ; having: expr_t option
  }

type select_stmt_t =
  { table: selectable_t
  ; fields: field_t list
  ; joins: join_stmt_t list
  ; where: where_stmt_t option
  ; order_by: order_by_stmt_t option
  ; group_by: group_by_stmt_t option
  ; limit: token_type_t option
  ; offset: token_type_t option
  ; distinct: bool
  }
and join_stmt_t =
  { join_type: join_type_t
  ; table: selectable_t
  ; condition: expr_t
  }
and selectable_t =
  | Table of { name: string; alias: string option }
  | Subselect of { select: select_stmt_t; alias: string option }
  | Subjoin of
    { lhs: selectable_t
    ; rhs: selectable_t
    ; join_type: join_type_t
    ; condition: expr_t
    ; alias: string option
    }

let parser_unexpected_token token msg =
  Error (Printf.sprintf "%sUnexpected token %s" msg (sprint_token token))

let parser_optional (f: parser_t -> 'a parser_result_t): parser_t -> ('a option) parser_result_t =
  fun p ->
    match f p with
    | Ok (p', r) -> Ok (p', Some r)
    | Error _ -> Ok (p, None)

let parser_if_some (v: 'a option) (f: parser_t -> 'b parser_result_t): parser_t -> 'b option parser_result_t =
  fun p ->
    match v with
    | Some _ -> (
      match f p with
      | Ok (p', r) -> Ok (p', Some r)
      | Error e -> Error e
    )
    | None -> Ok (p, None)

let parser_if_some2 (v: 'a option) (f: 'a -> parser_t -> 'b parser_result_t): parser_t -> 'b option parser_result_t =
  fun p ->
    match v with
    | Some v' -> (
      match f v' p with
      | Ok (p', r) -> Ok (p', Some r)
      | Error e -> Error e
    )
    | None -> Ok (p, None)

let parser_get_token (p: parser_t): token_t =
  if p.token_count > p.pos then
    p.tokens.(p.pos)
  else
    p.tokens.(p.token_count - 1)

let parser_peek_token (p: parser_t): token_t =
  if p.token_count > p.pos + 1 then
    p.tokens.(p.pos + 1)
  else
    p.tokens.(p.token_count - 1)

let parser_advance (p: parser_t): parser_t =
  { p with pos = p.pos + 1 }

let parser_expect (tt: token_type_t) (p: parser_t): token_type_t parser_result_t =
  let token = parser_get_token p in
    if token_type_cmp token.ttype tt then
      Ok (parser_advance p, token.ttype)
    else
      parser_unexpected_token token "1.. "

let parser_expect_oneof (tts: token_type_t list) (p: parser_t): token_type_t parser_result_t =
  let token = parser_get_token p in
  let found_opt = List.find_opt (token_type_cmp token.ttype) tts in
    match found_opt with
    | Some tt -> Ok (parser_advance p, tt)
    | None ->
      let tts_msg = join ", " sprint_token_type tts in
        parser_unexpected_token token ("4.. " ^ tts_msg ^ " ")

let parser_expect_exact (tts: token_type_t list) (p: parser_t): (token_type_t list) parser_result_t =
  let rec loop p' tts result =
    let token = parser_get_token p' in
    match tts with
    | [] -> Ok (p', List.rev result)
    | hd :: tl ->
      if token_type_cmp token.ttype hd then
        loop (parser_advance p') tl (hd :: result)
      else
        parser_unexpected_token token ("7.. ")
  in
    loop p tts []

let parser_ident (p: parser_t): string parser_result_t =
  let token = parser_get_token p in
    match token.ttype with
    | Identifier v -> Ok (parser_advance p, v)
    | _ ->
      parser_unexpected_token token "2.. "

let op_list = [Plus; Minus; Star; Slash ;Gt; Ge; Lt; Le; Equal; Like; ILike; In; NotIn]

let rec parser_expr (p: parser_t): expr_t parser_result_t =
  let token = parser_get_token p in
    match token.ttype with
    | OpenParen -> 
      let* p', expr = parser_expr (parser_advance p) in
      let* p', _ = parser_expect CloseParen p' in
        Ok (p', Parentheses expr)
    | String _ | Number _ | Identifier _ -> begin
      let* p', op_opt =
        (parser_advance p) |> parser_optional (parser_expect_oneof op_list)
      in
        match op_opt with
        | Some In ->
          let* p', _ = parser_expect OpenParen p' in
          let* p', values = parser_expr_list p' in
          let* p', _ = parser_expect CloseParen p' in
          let expr =
            InList { name = Value token.ttype; op = In; values = values }
          in
            Ok (p', expr)
        | Some NotIn ->
          let* p', _ = parser_expect OpenParen p' in
          let* p', values = parser_expr_list p' in
          let* p', _ = parser_expect CloseParen p' in
          let expr =
            InList { name = Value token.ttype; op = NotIn; values = values }
          in
            Ok (p', expr)
        | Some op ->
          let* p', rhs = parser_expr p' in
          let expr =
            BinOp { op = op; lhs = Value token.ttype; rhs = rhs }
          in
            Ok (p', expr)
        | None -> Ok (p', Value token.ttype)
    end
    | _ -> 
      parser_unexpected_token token "5.. "

and parser_expr_list (p: parser_t): (expr_t list) parser_result_t =
  let rec loop p' result =
    let* p', expr = parser_expr p' in
    let result = expr :: result in
    let token = parser_get_token p' in
      match token.ttype with
      | Comma -> loop (parser_advance p') result
      | _ -> Ok (p', List.rev result)
  in
    loop p []

let parser_field_list (parser1: parser_t): (field_t list) parser_result_t =
  let rec loop p result =
    let token = parser_get_token p in
      match token.ttype with
      | Star -> begin
        let field_expr = Value Star in
        let field = { expr = field_expr; alias = None } in
        let p' = parser_advance p in
          let token = parser_get_token p' in
            match token.ttype with
            | Comma -> loop (parser_advance p') (field :: result)
            | _ -> Ok (p', List.rev (field :: result))
      end
      | String _
      | Number _
      | Identifier _ -> begin
        let* p', field_expr = parser_expr p in
        let field = { expr = field_expr; alias = None } in
        let token = parser_get_token p' in
          match token.ttype with
          | Comma -> loop (parser_advance p') (field :: result)
          | As ->
            let p' = parser_advance p' in
            let* p', alias = parser_ident p' in
            let field = { field with alias = (Some alias) } in
            let result = field :: result in
            let token = parser_get_token p' in
              if token_type_cmp token.ttype Comma then
                loop (parser_advance p') result
              else
                Ok (p', List.rev result)
          | Identifier alias ->
            let field = { field with alias = (Some alias) } in
            let result = field :: result in
            let p' = parser_advance p' in
            let token = parser_get_token p' in
              if token_type_cmp token.ttype Comma then
                loop (parser_advance p') result
              else
                Ok (p', List.rev result)
          | _ -> Ok (p', List.rev (field :: result))
      end
      | _ ->
        parser_unexpected_token token "3.. "
  in
    loop parser1 []

let parser_order_by (p: parser_t): (order_by_stmt_t option) parser_result_t =
  let rec loop p' result =
    let* p', expr = parser_expr p' in
    let* p', kind = p' |> parser_optional (parser_expect_oneof [Asc; Desc]) in
    let ascending =
      match kind with
      | Some Desc -> false
      | _ -> true
    in
    let item =
      { expr = expr; ascending = ascending }
    in
    let result = item :: result in
    let* p', opt = p' |> parser_optional (parser_expect Comma) in
      match opt with
      | Some _ -> loop p' result
      | None ->
        let order_by = { items = (List.rev result) } in
          Ok (p', Some order_by)
  in
    let* p, opt = p |> parser_optional (parser_expect OrderBy) in
      match opt with
      | Some _ -> loop p []
      | None ->
        Ok (p, None)

let parser_group_by (p: parser_t): (group_by_stmt_t option) parser_result_t =
  let rec read_names p' result =
    let* p', name = parser_ident p' in
    let result = name :: result in
    let* p', opt = p' |> parser_optional (parser_expect Comma) in
      match opt with
      | Some _ -> read_names p' result
      | None -> Ok (p', List.rev result)
  in
    let* p, opt = p |> parser_optional (parser_expect GroupBy) in
      match opt with
      | Some _ -> (
        let* p, names = read_names p [] in
        let* p, having_opt = p |> parser_optional (parser_expect Having) in
          match having_opt with
          | None ->
            Ok (p, Some { names = names; having = None })
          | Some _ ->
            let* p, expr = parser_expr p in
              Ok (p, Some { names = names; having = (Some expr) })
      )
      | None -> Ok (p, None)

let join_list: token_type_t list =
  [InnerJoin; LeftJoin; RightJoin; LeftOuterJoin; RightOuterJoin]

let parser_where (p: parser_t): (where_stmt_t option) parser_result_t =
  let* p, opt = p |> parser_optional (parser_expect Where) in
    match opt with
    | Some _ ->
      let* p, expr = parser_expr p in
        Ok (p, Some { expr = expr })
    | None ->
      Ok (p, None)

let rec parser_single_select p =
  let* p, _ = parser_expect Select p in
  let* p, distinct_opt = p |> parser_optional (parser_expect Distinct) in
  let* p, fields = parser_field_list p in
  let* p, _ = parser_expect From p in
  let* p, table = parser_selectable p in
  let* p, joins = parser_joins p in
  let* p, where_opt = parser_where p in
  let* p, order_by_opt = parser_order_by p in
  let* p, group_by_opt = parser_group_by p in
  let* p, opt = p |> parser_optional (parser_expect Limit) in
  let* p, limit_opt =
    p |> parser_if_some opt (parser_expect (Number ""))
  in
  let* p, opt = p |> parser_optional (parser_expect Offset) in
  let* p, offset_opt =
    p |> parser_if_some opt (parser_expect (Number ""))
  in
  let select_stmt =
    { table = table
    ; fields = fields
    ; joins = joins
    ; order_by = order_by_opt
    ; group_by = group_by_opt
    ; where = where_opt
    ; limit = limit_opt
    ; offset = offset_opt 
    ; distinct = Option.is_some distinct_opt
    }
  in
    Ok (p, select_stmt)
and parser_joins (p: parser_t): (join_stmt_t list) parser_result_t =
  let rec loop p' joins =
    let* p', tt =
      p' |> parser_optional (parser_expect_oneof join_list)
    in
    match tt with
    | Some tt ->
      let* p', table = parser_selectable p' in
      let* p', _ = parser_expect On p' in
      let* p', expr = parser_expr p' in
      let join = 
        { join_type = (to_join_type tt)
        ; table = table
        ; condition = expr
        }
      in
        loop p' (join :: joins)
    | None -> Ok (p', List.rev joins)
  in
    loop p []
and parser_selectable p =
  let token = parser_get_token p in
  let p' = parser_advance p in
  match token.ttype with
  | Identifier name -> begin
    let* p', opt = p' |> parser_optional (parser_expect As) in
    match opt with
    | Some _ ->
      let* p', alias = parser_ident p' in
      let selectable =
        Table { name = name; alias = Some alias }
      in
        Ok (p', selectable)
    | None ->
      let* p', alias_opt = p' |> parser_optional parser_ident in
      let selectable =
        Table { name = name; alias = alias_opt }
      in
        Ok (p', selectable)
  end
  | OpenParen -> begin
    let token = parser_get_token p' in
    match token.ttype with
    | OpenParen
    | Identifier _ ->
      let* p', lhs = parser_selectable p' in
      let* p', tt = parser_expect_oneof join_list p' in
      let* p', rhs = parser_selectable p' in
      let* p', _ = parser_expect On p' in
      let* p', expr = parser_expr p' in
      let* p', _ = parser_expect CloseParen p' in
      let join_type = to_join_type tt in
      let* p', opt = p' |> parser_optional (parser_expect As) in
        (match opt with
        | Some _ ->
          let* p', alias = parser_ident p' in
          let selectable =
            Subjoin
              { lhs = lhs
              ; rhs = rhs
              ; join_type = join_type
              ; condition = expr
              ; alias = Some alias
            }
          in
            Ok (p', selectable)
        | None ->
          let* p', alias_opt = p' |> parser_optional parser_ident in
          let selectable =
            Subjoin
              { lhs = lhs
              ; rhs = rhs
              ; join_type = join_type
              ; condition = expr
              ; alias = alias_opt
            }
          in
            Ok (p', selectable)
        )
    | Select -> begin
      let* p', select = parser_single_select p' in
      let* p', _ = parser_expect CloseParen p' in
      let* p', opt = p' |> parser_optional (parser_expect As) in
      match opt with
      | Some _ ->
        let* p', alias = parser_ident p' in
        let selectable =
          Subselect { select = select; alias = Some alias }
        in
          Ok (p', selectable)
      | None ->
        let* p', alias_opt = p' |> parser_optional parser_ident in
        let selectable =
          Subselect { select = select; alias = alias_opt }
        in
          Ok (p', selectable)
    end
    | _ ->
      parser_unexpected_token token "6.. "
  end
  | _ ->
    parser_unexpected_token token "8.. "

let parser_select p =
  let* p, select_stmt = parser_single_select p in
  let* p, _ = p |> parser_optional (parser_expect Comma) in
    Ok (p, select_stmt)

let rec print_expr expr depth =
  if depth > 0 then (
    Printf.printf "%s" (
      List.init depth (fun _ -> " ")
      |> List.fold_left (fun r v -> r ^ v) ""
    )
  );

  match expr with
  | Value tt ->
    Printf.printf "%s\n" (sprint_token_type tt)
  | Parentheses expr -> 
    Printf.printf "()\n";
    print_expr expr (depth + 2)
  | BinOp binop ->
    Printf.printf "%s\n" (sprint_token_type binop.op);
    print_expr binop.lhs (depth + 2);
    print_expr binop.rhs (depth + 2)
  | InList l ->
    Printf.printf "%s\n" (sprint_token_type l.op);
    List.iter (fun v -> print_expr v (depth + 2)) l.values

let rec print_selectable (s: selectable_t) (depth: int): unit =
  let depth_str = Printf.sprintf "%*s" depth "" in
  match s with
  | Table t -> (
    match t.alias with
    | Some alias ->
      Printf.printf "%s" depth_str;
      Printf.printf "table: %s as %s\n" t.name alias
    | None ->
      Printf.printf "%s" depth_str;
      Printf.printf "table: %s\n" t.name
  )
  | Subselect s -> (
    (match s.alias with
    | Some alias ->
      Printf.printf "%s" depth_str;
      Printf.printf "from select as: %s\n" alias
    | None ->
      Printf.printf "%s" depth_str;
      Printf.printf "from select without alias\n"
    );
    print_select s.select (depth + 2)
  )
  | Subjoin s -> (
    Printf.printf "%s" depth_str;
    Printf.printf "%s\n" (sprint_token_type (from_join_type s.join_type));
    print_selectable s.lhs (depth + 2);
    print_selectable s.rhs (depth + 2);
    Printf.printf "%s" depth_str;
    Printf.printf " on\n";
    print_expr s.condition (depth + 2)
  )
and print_select (s: select_stmt_t) (depth: int): unit =
  let depth_str = Printf.sprintf "%*s" depth ""
  in

  Printf.printf "%s" depth_str;
  Printf.printf "select %s\n" (if s.distinct then "distinct" else "");

  print_selectable s.table (depth + 2);

  Printf.printf "%s" depth_str;
  Printf.printf "fields:\n";
  List.iter (
    fun field ->
      match field.alias with
      | Some v ->
        Printf.printf "%s" depth_str;
        Printf.printf "  alias = %s:\n" v;
        print_expr field.expr (depth + 4)
      | None ->
        print_expr field.expr (depth + 2)
  )
  s.fields;

  List.iter (
    fun v ->
      Printf.printf "%s" depth_str;
      Printf.printf "%s\n" (sprint_token_type (from_join_type v.join_type));

      print_selectable v.table (depth + 2);
      Printf.printf "%s" depth_str;
      Printf.printf " on\n";
      print_expr v.condition (depth + 2)
  )
  s.joins;

  (match s.where with
  | Some w ->
    Printf.printf "%s" depth_str;
    Printf.printf "where:\n";
    print_expr w.expr (depth + 2)
  | None -> ()
  );

  (match s.order_by with
  | Some o ->
    Printf.printf "%s" depth_str;
    Printf.printf "order by:\n";
    List.iter (
      fun item ->
        Printf.printf "%s" depth_str;
        Printf.printf "  %s\n" (if item.ascending then "asc" else "desc");
        print_expr item.expr (depth + 3)
    )
    o.items
  | None -> ()
  );

  (match s.group_by with
  | Some g -> (
    Printf.printf "%s" depth_str;
    Printf.printf "group by: %s\n" (join ", " (fun a -> a) g.names);
    match g.having with
    | Some e ->
      Printf.printf "%s" depth_str;
      Printf.printf "having:\n";
      print_expr e (depth + 2)
    | None -> ()
  )
  | None -> ()
  );

  (match s.limit with
  | Some l ->
    Printf.printf "%s" depth_str;
    Printf.printf "limit: %s\n" (sprint_token_type l)
  | None -> ()
  );

  match s.offset with
  | Some o ->
    Printf.printf "%s" depth_str;
    Printf.printf "offset: %s\n" (sprint_token_type o)
  | None -> ()
  
let rec format_expr expr depth =
  Printf.printf "%*s" depth "";

  match expr with
  | Value Identifier v -> Printf.printf "%s" v
  | Value Number v -> Printf.printf "%s" v
  | Value String v -> Printf.printf "%s" v
  | Value Star -> Printf.printf "*"
  | Parentheses expr -> 
    Printf.printf "(";
    format_expr expr 0;
    Printf.printf ")";
  | BinOp binop ->
    format_expr binop.lhs 0;
    Printf.printf " %s " (sprint_token_name_or_symbol binop.op);
    format_expr binop.rhs 0
  | InList l ->
    format_expr l.name 0;
    (match l.op with
    | In -> Printf.printf " IN "
    | NotIn -> Printf.printf " NOT IN "
    | _ -> failwith "format expr unreachable(3)"
    );

    Printf.printf "(";

    let first = ref true in
    List.iter
      (fun v ->
        if !first then (
          first := false;
          format_expr v 0
        ) else (
          Printf.printf ",";
          format_expr v 0
        )
      )
      l.values;

    Printf.printf ")"
  | _ -> failwith "format expr unreachable(2)"

let rec format_selectable (s: selectable_t) (depth: int): unit =
  let depth_str = Printf.sprintf "%*s" depth "" in
  match s with
  | Table t -> (
    match t.alias with
    | Some alias ->
      Printf.printf "%s AS %s" t.name alias
    | None ->
      Printf.printf "%s" t.name
  )
  | Subselect s -> (
    Printf.printf "(\n";
    format_select s.select (depth + 2);
    Printf.printf "\n)"
  )
  | Subjoin s -> (
    Printf.printf "(\n";
    Printf.printf "%s" depth_str;
    format_selectable s.lhs depth;
    Printf.printf " %s " (sprint_token_name_or_symbol (from_join_type s.join_type));
    format_selectable s.rhs depth;
    (match s.alias with
    | Some alias ->
      Printf.printf " AS %s" alias
    | None -> ()
    );
    Printf.printf "\n%s" depth_str;
    Printf.printf "ON ";
    format_expr s.condition 0;
    Printf.printf "\n)"
  )
and format_select (s: select_stmt_t) (depth: int): unit =
  let depth_str = Printf.sprintf "%*s" depth ""
  in

  Printf.printf "%s" depth_str;
  Printf.printf "SELECT %s \n" (if s.distinct then "DISTINCT " else "");

  let first = ref true in
  List.iter (
    fun field ->
      (if !first then
        first := false
       else
        Printf.printf ",\n"
      );

      match field.alias with
      | Some v ->
        format_expr field.expr (depth + 2);
        Printf.printf " AS %s" v
      | None ->
        format_expr field.expr (depth + 2)
  )
  s.fields;

  Printf.printf "\n%sFROM " depth_str;

  format_selectable s.table (depth + 2);

  List.iter (
    fun v ->
      Printf.printf "\n";
      Printf.printf "%s" depth_str;
      Printf.printf "%s " (sprint_token_name_or_symbol (from_join_type v.join_type));

      format_selectable v.table (depth + 2);
      Printf.printf "\n%s" depth_str;
      Printf.printf "ON ";
      format_expr v.condition 0
  )
  s.joins;

  (match s.where with
  | Some w ->
    Printf.printf "\n%s" depth_str;
    Printf.printf "WHERE ";
    format_expr w.expr 0;
    Printf.printf "\n"
  | None -> ()
  );

  (match s.order_by with
  | Some o ->
    Printf.printf "%s" depth_str;
    Printf.printf "ORDER BY ";
    let first = ref true in
      List.iter (
        fun item ->
          (if !first then
            first := false
           else
            Printf.printf ","
          );
          format_expr item.expr 0;
          if item.ascending then
            Printf.printf " ASC"
          else
            Printf.printf " DESC"
      )
      o.items;
      Printf.printf "\n"
  | None -> ()
  );

  (match s.group_by with
  | Some g -> (
    Printf.printf "%s" depth_str;
    Printf.printf "GROUP BY %s\n" (join ", " (fun a -> a) g.names);
    match g.having with
    | Some e ->
      Printf.printf "%s" depth_str;
      Printf.printf "HAVING \n";
      format_expr e (depth + 2)
    | None -> ()
  )
  | None -> ()
  );

  (match s.limit with
  | Some l ->
    Printf.printf "%s" depth_str;
    Printf.printf "LIMIT %s\n" (sprint_token_name_or_symbol l)
  | None -> ()
  );

  match s.offset with
  | Some o ->
    Printf.printf "%s" depth_str;
    Printf.printf "OFFSET %s\n" (sprint_token_name_or_symbol o)
  | None -> ()

let () =
  (*let input = "select *, abc as nwa, x.y from restaurants as r inner join settings as s on restaurant_id = id where x + 1 order by x limit 12 offset 100;"
  in *)
  let input = "
    select *, abc as nwa, x.y from restaurants as r
    inner join settings as s on restaurant_id = id
    inner join (select u, v from uv) as s on restaurant_id = id
    inner join (tbl1 left outer join tbl2 on p = q) as test on restaurant_id = id
    where x + 1 > 2
    order by x
    limit 12
    offset 100;
  "
  in
  (*let input = "
  SELECT \"IndividualBill\".\"id\", \"rescues\".\"id\" AS \"rescues.id\", \"rescues\".\"cashback\" AS \"rescues.cashback\", \"rescues\".\"status\" AS \"rescues.status\", \"session\".\"id\" AS \"session.id\", \"session\".\"key\" AS \"session.key\", \"session\".\"nfce_allowed\" AS \"session.nfce_allowed\", \"session\".\"user_change\" AS \"session.user_change\", \"session\".\"with_withdrawal\" AS \"session.with_withdrawal\", \"session\".\"delivery_tax_price\" AS \"session.delivery_tax_price\", \"session\".\"total_price\" AS \"session.total_price\", \"session\".\"total_delivery_price\" AS \"session.total_delivery_price\", \"session\".\"attendance_password\" AS \"session.attendance_password\", \"session\".\"is_delivery\" AS \"session.is_delivery\", \"session\".\"discount_total\" AS \"session.discount_total\", \"session\".\"ifood_id\" AS \"session.ifood_id\", \"session\".\"ifood_discount\" AS \"session.ifood_discount\", \"session\".\"merchant_discount\" AS \"session.merchant_discount\", \"session\".\"ifood_document\" AS \"session.ifood_document\", \"session\".\"ifood_paid\" AS \"session.ifood_paid\", \"session\".\"additional_fees\" AS \"session.additional_fees\", \"session\".\"ready_at\" AS \"session.ready_at\", \"session\".\"accepted_at\" AS \"session.accepted_at\", \"session\".\"ongoing_at\" AS \"session.ongoing_at\", \"session\".\"ifood_delivery_time\" AS \"session.ifood_delivery_time\", \"session\".\"scheduled_to\" AS \"session.scheduled_to\", \"session\".\"discount_obs\" AS \"session.discount_obs\", \"session\".\"old_total_price\" AS \"session.old_total_price\", \"session\".\"details\" AS \"session.details\", \"session\".\"ifood_on_demand_id\" AS \"session.ifood_on_demand_id\", \"session\".\"delivery_by\" AS \"session.delivery_by\", \"session\".\"delivery_fee_discount\" AS \"session.delivery_fee_discount\", \"session\".\"total_paid\" AS \"session.total_paid\", \"session\".\"foody_delivery_session_id\" AS \"session.foody_delivery_session_id\", \"session\".\"neemo_id\" AS \"session.neemo_id\", \"session\".\"sales_channel\" AS \"session.sales_channel\", \"session->table\".\"id\" AS \"session.table.id\", \"session->table\".\"table_number\" AS \"session.table.table_number\", \"session->table\".\"status\" AS \"session.table.status\", \"session->table\".\"table_type\" AS \"session.table.table_type\", \"session->payments\".\"id\" AS \"session.payments.id\", \"session->payments\".\"payment_value\" AS \"session.payments.payment_value\", \"session->payments\".\"payment_method_id\" AS \"session.payments.payment_method_id\", \"session->payments\".\"created_at\" AS \"session.payments.created_at\", \"session->payments->payment_method\".\"id\" AS \"session.payments.payment_method.id\", \"session->payments->payment_method\".\"name\" AS \"session.payments.payment_method.name\", \"session->payment_method\".\"id\" AS \"session.payment_method.id\", \"session->payment_method\".\"name\" AS \"session.payment_method.name\", \"session->ifood_restaurant\".\"id\" AS \"session.ifood_restaurant.id\", \"session->ifood_restaurant\".\"name\" AS \"session.ifood_restaurant.name\", \"session->motoboy\".\"id\" AS \"session.motoboy.id\", \"session->motoboy\".\"name\" AS \"session.motoboy.name\", \"session->motoboy\".\"phone\" AS \"session.motoboy.phone\", \"session->buyer_address\".\"id\" AS \"session.buyer_address.id\", \"session->buyer_address\".\"state\" AS \"session.buyer_address.state\", \"session->buyer_address\".\"city\" AS \"session.buyer_address.city\", \"session->buyer_address\".\"neighborhood\" AS \"session.buyer_address.neighborhood\", \"session->buyer_address\".\"street\" AS \"session.buyer_address.street\", \"session->buyer_address\".\"number\" AS \"session.buyer_address.number\", \"session->buyer_address\".\"complement\" AS \"session.buyer_address.complement\", \"session->buyer_address\".\"reference\" AS \"session.buyer_address.reference\", \"session->buyer_address\".\"zip_code\" AS \"session.buyer_address.zip_code\", \"session->buyer_address\".\"longitude\" AS \"session.buyer_address.longitude\", \"session->buyer_address\".\"latitude\" AS \"session.buyer_address.latitude\", \"order_baskets\".\"id\" AS \"order_baskets.id\", \"order_baskets\".\"order_status\" AS \"order_baskets.order_status\", \"order_baskets\".\"basket_id\" AS \"order_baskets.basket_id\", \"order_baskets\".\"total_price\" AS \"order_baskets.total_price\", \"order_baskets\".\"total_service_price\" AS \"order_baskets.total_service_price\", \"order_baskets\".\"start_time\" AS \"order_baskets.start_time\", \"order_baskets\".\"close_time\" AS \"order_baskets.close_time\", \"order_baskets\".\"canceled_at\" AS \"order_baskets.canceled_at\", \"order_baskets\".\"ifood_id\" AS \"order_baskets.ifood_id\", \"order_baskets\".\"schedule\" AS \"order_baskets.schedule\", \"order_baskets\".\"ifood_table\" AS \"order_baskets.ifood_table\", \"order_baskets\".\"command_table_number\" AS \"order_baskets.command_table_number\", \"order_baskets\".\"scheduled_to\" AS \"order_baskets.scheduled_to\", \"order_baskets\".\"neemo_id\" AS \"order_baskets.neemo_id\", \"order_baskets->waiter\".\"id\" AS \"order_baskets.waiter.id\", \"order_baskets->waiter\".\"name\" AS \"order_baskets.waiter.name\", \"waiter\".\"id\" AS \"waiter.id\", \"waiter\".\"name\" AS \"waiter.name\", \"waiter\".\"is_protected\" AS \"waiter.is_protected\", \"buyer\".\"id\" AS \"buyer.id\", \"buyer\".\"name\" AS \"buyer.name\", \"buyer\".\"phone\" AS \"buyer.phone\", \"buyer\".\"email\" AS \"buyer.email\", \"buyer\".\"ifood_phone\" AS \"buyer.ifood_phone\", \"buyer\".\"neemo_phone\" AS \"buyer.neemo_phone\", \"buyer\".\"localizer\" AS \"buyer.localizer\" FROM \"individual_bills\" AS \"IndividualBill\" LEFT OUTER JOIN \"clube_rescues\" AS \"rescues\" ON \"IndividualBill\".\"id\" = \"rescues\".\"individual_bill_id\" INNER JOIN \"table_sessions\" AS \"session\" ON \"IndividualBill\".\"session_id\" = \"session\".\"id\" AND (\"session\".\"created_at\" BETWEEN '2025-11-16 12:49:18.038 +00:00' AND '2025-11-26 12:49:18.038 +00:00' OR \"session\".\"scheduled_to\" >= '2025-11-16 12:49:18.038 +00:00') AND \"session\".\"restaurant_id\" = 302 AND \"session\".\"status\" NOT IN ('completed', 'transfer') LEFT OUTER JOIN \"place_tables\" AS \"session->table\" ON \"session\".\"table_id\" = \"session->table\".\"id\" LEFT OUTER JOIN \"payments\" AS \"session->payments\" ON \"session\".\"id\" = \"session->payments\".\"table_session_id\" LEFT OUTER JOIN \"payment_methods\" AS \"session->payments->payment_method\" ON \"session->payments\".\"payment_method_id\" = \"session->payments->payment_method\".\"id\" LEFT OUTER JOIN \"payment_methods\" AS \"session->payment_method\" ON \"session\".\"intended_payment_method_id\" = \"session->payment_method\".\"id\" LEFT OUTER JOIN \"ifood_restaurants\" AS \"session->ifood_restaurant\" ON \"session\".\"ifood_restaurant_id\" = \"session->ifood_restaurant\".\"id\" LEFT OUTER JOIN \"motoboys\" AS \"session->motoboy\" ON \"session\".\"motoboy_id\" = \"session->motoboy\".\"id\" LEFT OUTER JOIN \"buyer_delivery_addresses\" AS \"session->buyer_address\" ON \"session\".\"buyer_delivery_address_id\" = \"session->buyer_address\".\"id\" INNER JOIN \"order_baskets\" AS \"order_baskets\" ON \"IndividualBill\".\"id\" = \"order_baskets\".\"bill_id\" AND \"order_baskets\".\"order_status\" IN ('canceled_waiting_payment', 'ready', 'pending', 'delivered', 'accepted', 'ongoing', 'canceled') LEFT OUTER JOIN \"waiters\" AS \"order_baskets->waiter\" ON \"order_baskets\".\"waiter_id\" = \"order_baskets->waiter\".\"id\" LEFT OUTER JOIN \"waiters\" AS \"waiter\" ON \"IndividualBill\".\"waiter_id\" = \"waiter\".\"id\" LEFT OUTER JOIN \"buyers\" AS \"buyer\" ON \"IndividualBill\".\"buyer_id\" = \"buyer\".\"id\";
  "
  in*)
  (*let input = "select a.*, b, c from (select * from t) as k inner join (select x, y from fears) as f on k.id = f.id;"
  in*)
  (*let input = "select distinct a from (today inner join tomorrow on a = b) where x not in (1, 2, 3)"
  in*)
  let ctx =
    { data = input
    ; pos = 0
    ; line = 1
    ; column = 1
    ; start = 0
    }
  in
  match tokenize ctx with
  | Ok tokens -> (
    (*List.iter print_token tokens;*)
    let parser' =
      { tokens = Array.of_list tokens
      ; token_count = List.length tokens
      ; pos = 0
      }
    in
    match parser_select parser' with
    | Error e -> Printf.printf "parse error %s\n" e
    | Ok (p, s) ->
      Printf.printf "parse ok\n";
      print_select s 0;
      Printf.printf "\n------\n\n";
      format_select s 0;
      Printf.printf "\n"
  )
  | Error e -> Printf.printf "%s\n" e

