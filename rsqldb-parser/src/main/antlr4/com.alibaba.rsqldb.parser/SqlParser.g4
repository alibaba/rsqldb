parser grammar SqlParser;

options { tokenVocab=SqlLexer; }

tokens {
    DELIMITER
}

sqlStatements
    : (sqlStatement)? SEMICOLON? EOF
    ;

sqlStatement
    : query                                                                                         #queryStatement
    | CREATE TABLE ifNotExists? tableName (tableDescriptor)? (WITH tableProperties)?                #createTable
    | CREATE VIEW  ifNotExists? viewName AS query                                                   #createView
    | INSERT INTO tableName query                                                                   #insertSelect
    | INSERT INTO tableName (tableDescriptor)? VALUES values                                        #insertValue
    ;

tableDescriptor
    : LR_BRACKET columnDescriptor (COMMA columnDescriptor)* RR_BRACKET
    ;

columnDescriptor
    : identifier (dataType)?
//    columnConstraint?
//    | (columnConstraint LR_BRACKET identifier RR_BRACKET)?
//    | asField
    ;

tableProperties
    : LR_BRACKET tableProperty (COMMA tableProperty)* RR_BRACKET
    ;

tableProperty
    : (identifier | STRING) EQUAL_SYMBOL literal
    ;

query
    : SELECT selectField FROM tableName (AS identifier)?
     (WHERE booleanExpression)?
     (GROUP BY (window_function COMMA)? fieldName (COMMA fieldName)*)?
     (HAVING booleanExpression)?
     ((LEFT)? JOIN tableName (AS identifier)? ON joinCondition)?
    ;

selectField
    : asField (COMMA asField)*
    | STAR
    ;

asField
    : identifier (AS identifier)?
    | fieldName (AS identifier)?
    | function (AS identifier)?
    | window_function (AS identifier)?
    ;

joinCondition
    : fieldName EQUAL_SYMBOL fieldName
    ;

booleanExpression
    : booleanExpression (AND | OR) booleanExpression
    | fieldName operator literal
    | fieldName IS NULL
    | fieldName BETWEEN NUMBER AND NUMBER
    | fieldName IN values
    ;

literal
    : NULL                                  #nullLiteral
    | (TRUE | FALSE)                        #booleanLiteral
    | STRING                                #stringLiteral
    | VARIABLE                              #variableLiteral
    | NUMBER                                #numberLiteral
    | QUOTED_NUMBER                         #quotedNumberLiteral
    | QUOTED_STRING                         #quotedStringLiteral
    ;

function
    : calculator LR_BRACKET (fieldName | STAR) RR_BRACKET
    ;

calculator
    : AVG                                   #avgCalculator
    | COUNT                                 #countCalculator
    | MAX                                   #maxCalculator
    | MIN                                   #minCalculator
    | SUM                                   #sumCalculator
    ;

window_function
    : tumble_window
    | hop_window
    | session_window
    ;

//TUMBLE_START(ts, INTERVAL '1' MINUTE)
tumble_window
    : (TUMBLE | TUMBLE_START | TUMBLE_END) LR_BRACKET fieldName COMMA INTERVAL QUOTED_NUMBER timeunit RR_BRACKET
    ;
// HOP_START (ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)
hop_window
    : (HOP | HOP_START | HOP_END) LR_BRACKET fieldName COMMA INTERVAL QUOTED_NUMBER timeunit COMMA INTERVAL QUOTED_NUMBER timeunit RR_BRACKET
    ;
//SESSION_START(ts, INTERVAL '30' SECOND)
session_window
    : (SESSION | SESSION_START | SESSION_END) LR_BRACKET fieldName COMMA INTERVAL QUOTED_NUMBER timeunit RR_BRACKET
    ;

operator
    : EQUAL_SYMBOL | GREATER_SYMBOL | LESS_SYMBOL | NOT_EQUAL_SYMBOL | GREATER_EQUAL_SYMBOL | LESS_EQUAL_SYMBOL
    ;

timeunit
    : DAY | HOUR | MINUTE | SECOND | MILLISECOND
    ;



values
    : LR_BRACKET literal (COMMA literal)* RR_BRACKET
    ;

columnConstraint
    : ((PRIMARY)? KEY)
    ;
fieldName
    : identifier
    | tableName DOT identifier
    ;

dataType
    : identifier
    ;

tableName
    : identifier
    ;

viewName
    : identifier
    ;

identifier
    : ALPHABET_STRING                   #alphabetIdentifier
    | NUM_STRING                        #numIdentifier
    | QUOTED_STRING                     #quotedIdentifier
    | BACKQUOTED_STRING                 #backQuotedIdentifier
    | STRING                            #stringIdentifier
    | VARIABLE                          #variable
    ;

nonReserved
    : IF
    | SOURCE | SINK
    | PRIMARY | KEY
    | EMIT
    | CHANGES
    ;

ifExists
    : IF EXISTS;

ifNotExists
    : IF NOT EXISTS;
