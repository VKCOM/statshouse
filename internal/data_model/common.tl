//! common/tl/schemas/common.tl

/////
//
// Common Types
//
/////

int#a8509bda ? = Int;       // TODO - remove
string#b5286e24 ? = String; // TODO - remove

// Vector
vector#1cb5c415 {t:Type} # [t] = Vector t;
tuple#9770768a {t:Type} {n:#} [t] = Tuple t n;
dictionaryField {t:Type} key:string value:t = DictionaryField t;
dictionary#1f4c618f {t:Type} %(Vector %(DictionaryField t)) = Dictionary t;

resultFalse#27930a7b {t:Type} = Maybe t;
resultTrue#3f9c8ef8 {t:Type} result:t = Maybe t;

pair {X:Type} {Y:Type} a:X b:Y = Pair X Y;

map {X:Type} {Y:Type} key:X value:Y = Map X Y;

true = True; // this can be used as void type and serialized to empty array in PHP
