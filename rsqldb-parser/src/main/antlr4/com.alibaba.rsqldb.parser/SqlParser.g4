parser grammar SqlParser;

options { tokenVocab=SqlLexer; }

tokens {
    DELIMITER
}

sqlStatements
    : (sqlStatement)* EOF
    ;

sqlStatement
    : sqlBody SEMICOLON?
    ;

sqlBody
    : query                                                                                         #queryStatement
    | CREATE TABLE ifNotExists? tableName (tableDescriptor)? (WITH tableProperties)?                #createTable
    | CREATE VIEW  ifNotExists? viewName AS query                                                   #createView
    | INSERT INTO tableName (tableDescriptor)? query                                                #insertSelect
    | INSERT INTO tableName (tableDescriptor)? VALUES values                                        #insertValue
    ;

tableDescriptor
    : LR_BRACKET columnDescriptor (COMMA columnDescriptor)* RR_BRACKET
    ;

columnDescriptor
    : identifier (dataType)?                                                            #normalColumn
    | fieldName AS PROCTIME LR_BRACKET RR_BRACKET                                       #processTimeColumn
//    columnConstraint?
//    | (columnConstraint LR_BRACKET identifier RR_BRACKET)?
//    | asField
    ;

tableProperties
    : LR_BRACKET tableProperty (COMMA tableProperty)* RR_BRACKET
    ;

tableProperty
    : identifier EQUAL_SYMBOL value
    ;

query
    : SELECT selectField FROM tableName (AS identifier)?
     (wherePhrase)?
     (groupByPhrase)?
     (havingPhrase)?
     (joinPhrase (wherePhrase)? (groupByPhrase)? (havingPhrase)?)?
    ;

wherePhrase
    : WHERE booleanExpression
    ;
groupByPhrase
    : GROUP BY (windowFunction COMMA)? fieldName (COMMA fieldName)*
    ;
havingPhrase
    : HAVING booleanExpression
    ;
joinPhrase
    : (LEFT | INNER)? JOIN tableName (AS identifier)? ON joinCondition
    ;

selectField
    : asField (COMMA asField)*
    | STAR
    ;

asField
    : fieldName (AS identifier)?                                        #asFieldName
    | function (AS identifier)?                                         #asFunctionField
    | windowFunction (AS identifier)?                                   #asWindowFunctionField
    ;

joinCondition
    : oneJoinCondition (AND oneJoinCondition)*
    ;

oneJoinCondition
    : fieldName EQUAL_SYMBOL fieldName
    ;

booleanExpression
    : booleanExpression (AND | OR) booleanExpression                    #jointExpression
    | fieldName operator value                                        #operatorExpression
    | fieldName IS NULL                                                 #isNullExpression
    | fieldName BETWEEN NUMBER AND NUMBER                               #betweenExpression
    | fieldName IN values                                               #inExpression
    | function operator value                                         #functionExpression
    | (BINARY)? fieldName LIKE wildcard                                #wildcardExpression
    ;

value
    : NULL                                  #nullValue
    | (TRUE | FALSE)                        #booleanValue
    | STRING                                #stringValue
    | VARIABLE                              #variableValue
    | NUMBER                                #numberValue
    | QUOTED_NUMBER                         #quotedNumberValue
    | QUOTED_STRING                         #quotedStringValue
    | BACKQUOTED_STRING                     #backQuotedStringValue
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

windowFunction
    : tumble_window                         #tumbleWindow
    | hop_window                            #hopWindow
    | session_window                        #sessionWindow
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

wildcard
    : (STRING | QUOTED_STRING)                          #likeWildcard
    ;

values
    : LR_BRACKET value (COMMA value)* RR_BRACKET
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
