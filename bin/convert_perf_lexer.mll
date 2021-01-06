{
  type sample = { time: float; backtrace: entry list }
  and entry = { addr: Int64.t; symbol: string; source: string }
}

let hex = ['0' - '9' 'a' - 'f']+
rule read_sample = parse
  | [^ ' ']* ' '+
    ['0'-'9']+ ' '+
    (['0'-'9' '.']+ as time) ':'
    [^ '\n']+ '\n'
    { { time = float_of_string time;
        backtrace = read_bt_entry lexbuf } }
  | eof { raise End_of_file }

and read_bt_entry = parse
  | '\t' ' '* (hex as addr) ' '
    ([^ ' ' '+']+ as symbol)
    ("+0x" hex)?
    ' '
    '(' ([^ ')']+ as source) ')'
    '\n'
    { let addr = Int64.of_string ("0x" ^ addr) in
      { addr; symbol; source } :: read_bt_entry lexbuf }
  | '\n' { [] }
