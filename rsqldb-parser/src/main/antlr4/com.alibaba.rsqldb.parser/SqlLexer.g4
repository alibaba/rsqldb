lexer grammar SqlLexer;


 @lexer::members {
     public static final int COMMENTS = 2;
     public static final int WHITESPACE = 3;
     public static final int COMMANDS = 4;
 }

// Operators
// Operators. Assigns

VAR_ASSIGN:                          ':=';
PLUS_ASSIGN:                         '+=';
MINUS_ASSIGN:                        '-=';
MULT_ASSIGN:                         '*=';
DIV_ASSIGN:                          '/=';
MOD_ASSIGN:                          '%=';
AND_ASSIGN:                          '&=';
XOR_ASSIGN:                          '^=';
OR_ASSIGN:                           '|=';

// Operators. Arithmetics

STAR:                                '*';
DIVIDE:                              '/';
MODULE:                              '%';
PLUS:                                '+';
MINUSMINUS:                          '--';
MINUS:                               '-';
DIV:                                 'DIV';
MOD:                                 'MOD';

// Operators. Comparation

EQUAL_SYMBOL:                        '=';
GREATER_SYMBOL:                      '>';
LESS_SYMBOL:                         '<';
NOT_EQUAL_SYMBOL:                    '<>'       |   '!=';
GREATER_EQUAL_SYMBOL:                '>=';
LESS_EQUAL_SYMBOL:                   '<=';

// Operators. Bit

BIT_NOT_OP:                          '~';
BIT_OR_OP:                           '|';
BIT_AND_OP:                          '&';
BIT_XOR_OP:                          '^';

// Constructors symbols

DOT:                                 '.';
LR_BRACKET:                          '(';
RR_BRACKET:                          ')';
COMMA:                               ',';
SEMICOLON:                           ';';

// Group function Keywords

AVG:                                    'AVG'      |       'avg';
COUNT:                                  'COUNT'    |       'count';
MAX:                                    'MAX'      |       'max';
MIN:                                    'MIN'      |       'min';
SUM:                                    'SUM'      |       'sum';


// Keywords
CREATE:                                 'CREATE'            |       'create';
INSERT:                                 'INSERT'            |       'insert';
TABLE:                                  'TABLE'             |       'table';
VIEW:                                   'VIEW'              |       'view';
SELECT:                                 'SELECT'            |       'select';
IF:                                     'IF'                |       'if';
EXISTS:                                 'EXISTS'            |       'exists';
NOT:                                    'NOT'               |       'not';
OR:                                     'OR'                |       'or';
REPLACE:                                'REPLACE'           |       'replace';
SOURCE:                                 'SOURCE'            |       'source';
SINK:                                   'SINK'              |       'sink';
PRIMARY:                                'PRIMARY'           |       'primary';
KEY:                                    'KEY'               |       'key';
EMIT:                                   'EMIT'              |       'emit';
CHANGES:                                'CHANGES'           |       'changes';
FALSE:                                  'FALSE'             |       'false';
TRUE:                                   'TRUE'              |       'true';
WHEN:                                   'WHEN'              |       'when';
WHERE:                                  'WHERE'             |       'where';
WITH:                                   'WITH'              |       'with';
NULL:                                   'NULL'              |       'null';
JOIN:                                   'JOIN'              |       'join';
GROUP:                                  'GROUP'             |       'group';
HAVING:                                 'HAVING'            |       'having';
FROM:                                   'FROM'              |       'from';
AS:                                     'AS'                |       'as';
BY:                                     'BY'                |       'by';
IS:                                     'IS'                |       'is';
VALUES:                                 'VALUES'            |       'values';
BETWEEN:                                'BETWEEN'           |       'between';
AND:                                    'AND'               |       'and';
IN:                                     'IN'                |       'in';
INTO:                                   'INTO'              |       'into';
LEFT:                                   'LEFT'              |       'left';
ON:                                     'ON'                |       'on';
YEAR:                                   'YEAR'              |       'year';
MONTH:                                  'MONTH'             |       'month';
DAY:                                    'DAY'               |       'day';
HOUR:                                   'HOUR'              |       'hour';
MINUTE:                                 'MINUTE'            |       'minute';
SECOND:                                 'SECOND'            |       'second';
MILLISECOND:                            'MILLISECOND'       |       'millisecond';
INTERVAL:                               'INTERVAL'          |       'interval';
TUMBLE:                                 'TUMBLE'            |       'tumble';
TUMBLE_START:                           'TUMBLE_START'      |       'tumble_start';
TUMBLE_END:                             'TUMBLE_END'        |       'tumble_end';
HOP:                                    'HOP'               |       'hop';
HOP_START:                              'HOP_START'         |       'hop_start';
HOP_END:                                'HOP_END'           |       'hop_end';
SESSION:                                'SESSION'           |       'session';
SESSION_START:                          'SESSION_START'     |       'session_start';
SESSION_END:                            'SESSION_END'       |       'session_end';


NUMBER
    : MINUS? DECIMAL
    | MINUS? INTEGER
    ;

INTEGER
    : NUM+
    ;

QUOTED_NUMBER
    : '\'' NUMBER '\''
    | '"' NUMBER '"'
    ;

DECIMAL
    : NUM+ DOT NUM*
    | DOT NUM+
    ;

//字母或者_开头的字符串
ALPHABET_STRING
    : (ALPHABET | '_') (ALPHABET | NUM | '_' | '@' )*
    ;

//数字开头的字符串，1w21 12221_ 12@ 不能是123
NUM_STRING
    : NUM (ALPHABET | NUM | '_' | '@' )*
    ;

//单引号字符串，'1se' 's2' '$%#w1' 但不能是'211'纯数字
STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

//双引号字符串，"11w" "w12" "^$%$$" 但不能是"211"纯数字
QUOTED_STRING
    : '"' ( ~'"' | '""' )* '"'
    ;

//双引号字符串，`11w` `w12` `^$%$$` ,可以是`211`
BACKQUOTED_STRING
    : '`' ( ~'`' | '``' )* '`'
    ;

VARIABLE
    : '${' STRING '}'
    ;

fragment ALPHABET
    : [a-zA-Z]
    ;

fragment EXPONENT
    : 'E' [+-]? NUM+
    ;

fragment NUM
    : [0-9]
    ;


LINE_COMMENT
    : (('--' ~'@' ~[\r\n]* '\r'? '\n'?) | ('--#' ~[\r\n]* '\r'? '\n'?)) -> channel(2) // channel(COMMENTS)
    ;

COMMENT
    : (('/*' .*? '*/') | ('/*!' .+? '*/')) -> channel(2)      // channel(COMMENTS)
    ;

COMMAND
    : '--@' ~[\r\n]* '\r'? '\n'? -> channel(4)          // channel(COMMANDS)
    ;
WS
    : [ \r\n\t]+ -> channel(3)              // channel(WHITESPACE)
    ;