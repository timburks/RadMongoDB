
#import "RadBSON.h"
#import "platform.h"
#include "bson.h"

@protocol NuCellProtocol
- (id) car;
- (id) cdr;
@end

@protocol NuSymbolProtocol
- (NSString *) labelName;
@end

@interface RadBSONObjectID ()
{
@public
    bson_oid_t oid;
    
}

- (const bson_oid_t *) objectIDPointer;
- (id) initWithObjectIDPointer:(const bson_oid_t *) objectIDPointer;
/*! Create an object ID wrapper from a bson_oid_t native structure. */
+ (RadBSONObjectID *) objectIDWithObjectIDPointer:(const bson_oid_t *) objectIDPointer;
/*! Raw object id */
- (bson_oid_t) oid;

@end

@interface RadBSON ()
{
@public
    bson bsonValue;
}
- (RadBSON *) initWithBSON:(bson) b;
@end

void add_object_to_bson(bson *b, id key, id object)
{
    const char *name = [key cStringUsingEncoding:NSUTF8StringEncoding];
    Class NuCell = NSClassFromString(@"NuCell");
    Class NuSymbol = NSClassFromString(@"NuSymbol");
    
    if ([object isKindOfClass:[NSNumber class]]) {
        const char *objCType = [object objCType];
        switch (*objCType) {
            case 'd':
            case 'f':
                bson_append_double(b, name, [object doubleValue]);
                break;
            case 'l':
            case 'L':
                bson_append_long(b, name, [object longValue]);
                break;
            case 'q':
            case 'Q':
                bson_append_long(b, name, [object longLongValue]);
                break;
            case 'B':
                bson_append_bool(b, name, [object boolValue]);
                break;
            case 'c':
            case 'C':
            case 's':
            case 'S':
            case 'i':
            case 'I':
            default:
                bson_append_int(b, name, [object intValue]);
                break;
        }
    }
    else if ([object isKindOfClass:[NSDictionary class]]) {
        bson_append_start_object(b, name);
        id keys = [object allKeys];
        for (int i = 0; i < [keys count]; i++) {
            id key = [keys objectAtIndex:i];
            add_object_to_bson(b, key, [object objectForKey:key]);
        }
        bson_append_finish_object(b);
    }
    else if ([object isKindOfClass:[NSArray class]]) {
        bson_append_start_array(b, name);
        for (int i = 0; i < [object count]; i++) {
            add_object_to_bson(b,
                               [[NSNumber numberWithInt:i] stringValue],
                               [object objectAtIndex:i]);
        }
        bson_append_finish_object(b);
    }
    else if ([object isKindOfClass:[NSNull class]]) {
        bson_append_null(b, name);
    }
    else if ([object isKindOfClass:[NSDate class]]) {
        bson_date_t millis = (bson_date_t) ([object timeIntervalSince1970] * 1000.0);
        bson_append_date(b, name, millis);
    }
    else if ([object isKindOfClass:[NSData class]]) {
        bson_append_binary(b, name, 0, [object bytes], [object length]);
    }
    else if ([object isKindOfClass:[RadBSONObjectID class]]) {
        bson_append_oid(b, name, [((RadBSONObjectID *) object) objectIDPointer]);
    }
    else if (NuCell && [object isKindOfClass:[NuCell class]]) {
        // serialize Nu code as binary data of type 1
        NSData *serialization = [NSKeyedArchiver archivedDataWithRootObject:object];
        bson_append_binary(b, name, 1, [serialization bytes], [serialization length]);
    }
    else if (NuSymbol && [object isKindOfClass:[NuSymbol class]]) {
        if ([[object stringValue] isEqualToString:@"t"]) {
            bson_append_bool(b, name, YES);
        }
    }
    else if ([object respondsToSelector:@selector(cStringUsingEncoding:)]) {
        bson_append_string(b, name, [object cStringUsingEncoding:NSUTF8StringEncoding]);
    }
    else {
        NSLog(@"We have a problem. %@ cannot be serialized to bson", object);
    }
}

@implementation RadBSONObjectID

+ (RadBSONObjectID *) objectID
{
    bson_oid_t oid;
    bson_oid_gen(&oid);
    return [[RadBSONObjectID alloc] initWithObjectIDPointer:&oid];
}

+ (RadBSONObjectID *) objectIDWithData:(NSData *) data
{
    return [[RadBSONObjectID alloc] initWithData:data];
}

+ (RadBSONObjectID *) objectIDWithObjectIDPointer:(const bson_oid_t *) objectIDPointer
{
    return [[RadBSONObjectID alloc] initWithObjectIDPointer:objectIDPointer];
}

- (id) initWithString:(NSString *) s
{
    if (self = [super init]) {
        bson_oid_from_string(&oid, [s cStringUsingEncoding:NSUTF8StringEncoding]);
    }
    return self;
}

- (id) initWithObjectIDPointer:(const bson_oid_t *) objectIDPointer
{
    if (self = [super init]) {
        oid = *objectIDPointer;
    }
    return self;
}

- (const bson_oid_t *) objectIDPointer {return &oid;}

- (bson_oid_t) oid {return oid;}

- (id) initWithData:(NSData *) data
{
    if (self = [super init]) {
        if ([data length] == 12) {
            memcpy(oid.bytes, [data bytes], 12);
        }
    }
    return self;
}

- (id) copyWithZone:(NSZone *) zone
{
	return [[[self class] allocWithZone:zone] initWithObjectIDPointer:&oid];
}

- (NSUInteger) hash {
	return oid.ints[0] + oid.ints[1] + oid.ints[2];
}

- (NSData *) dataRepresentation
{
    return [[NSData alloc] initWithBytes:oid.bytes length:12];
}

- (NSString *) description
{
    char buffer[25];                              /* str must be at least 24 hex chars + null byte */
    bson_oid_to_string(&oid, buffer);
    return [NSString stringWithFormat:@"(oid \"%s\")", buffer];
}

- (NSString *) stringValue
{
    char buffer[25];                              /* str must be at least 24 hex chars + null byte */
    bson_oid_to_string(&oid, buffer);
    return [NSString stringWithCString:buffer encoding:NSUTF8StringEncoding];
}

- (NSComparisonResult)compare:(RadBSONObjectID *) other
{
    for (int i = 0; i < 3; i++) {
        int diff = oid.ints[i] - other->oid.ints[i];
        if (diff < 0)
            return NSOrderedAscending;
        else if (diff > 0)
            return NSOrderedDescending;
    }
    return NSOrderedSame;
}

- (BOOL)isEqual:(id)other
{
    return ([self compare:other] == 0);
}

@end

@implementation RadBSON

+ (RadBSON *) bsonWithData:(NSData *) data
{
    return [[RadBSON alloc] initWithData:data];
}

+ (NSMutableArray *) bsonArrayWithData:(NSData *) data
{
    NSLog(@"beware of leaks...");
    NSMutableArray *results = [NSMutableArray array];
    bson bsonBuffer;
    bsonBuffer.data = (char *) [data bytes];
    // bsonBuffer.owned = NO;
    while (bson_size(&bsonBuffer)) {
        bson bsonValue;
        bson_copy(&bsonValue, &bsonBuffer);
        bsonBuffer.data += bson_size(&bsonValue);
        RadBSON *bsonObject = [[RadBSON alloc] initWithBSON:bsonValue];
        [results addObject:bsonObject];
    }
    bson_destroy(&bsonBuffer);
    return results;
}

+ (RadBSON *) bsonWithDictionary:(NSDictionary *) dictionary
{
    return [[RadBSON alloc] initWithDictionary:dictionary];
}

+ (RadBSON *) bsonWithList:(id) list
{
    return [[RadBSON alloc] initWithList:list];
}

// internal, takes ownership of argument
- (RadBSON *) initWithBSON:(bson) b
{
    if (self = [super init]) {
        bsonValue = b;
    }
    return self;
}

- (RadBSON *) initWithData:(NSData *) data
{
    if ((self = [super init])) {
        char *bytes = (char *) malloc ([data length] * sizeof(char));
        memcpy(bytes, [data bytes], [data length]); 
        bson_init_finished_data(&bsonValue, bytes);
    }
    return self;
}

- (NSData *) dataRepresentation
{
    return [[NSData alloc]
            initWithBytes:(bsonValue.data)
            length:bson_size(&(bsonValue))];
}

- (RadBSON *) initWithDictionary:(NSDictionary *) dict
{
    bson b;
    bson_init(&b);
    NSArray * keys = [dict allKeys];
    for (int i = 0; i < [keys count]; i++) {
        id key = [keys objectAtIndex:i];
        add_object_to_bson(&b, key, [dict objectForKey:key]);
    }
    bson_finish(&b);
    return [self initWithBSON:b];
}

- (RadBSON *) initWithList:(id) cell
{
    bson b;
    bson_init(&b);
    id cursor = cell;
    while (cursor && (cursor != [NSNull null])) {
        id key = [[cursor car] labelName];
        id value = [[cursor cdr] car];
        add_object_to_bson(&b, key, value);
        cursor = [[cursor cdr] cdr];
    }
    bson_finish(&b);
    return [self initWithBSON:b];
}

- (void) dealloc
{
    bson_destroy(&bsonValue);
}

void dump_bson_iterator(bson_iterator it, const char *indent)
{
    bson_iterator it2;
    bson subobject;
    
    char more_indent[2000];
    sprintf(more_indent, "  %s", indent);
    
    while(bson_iterator_next(&it)) {
        fprintf(stderr, "%s  %s: ", indent, bson_iterator_key(&it));
        char hex_oid[25];
        
        switch(bson_iterator_type(&it)) {
            case BSON_DOUBLE:
                fprintf(stderr, "(double) %e\n", bson_iterator_double(&it));
                break;
            case BSON_INT:
                fprintf(stderr, "(int) %d\n", bson_iterator_int(&it));
                break;
            case BSON_STRING:
                fprintf(stderr, "(string) \"%s\"\n", bson_iterator_string(&it));
                break;
            case BSON_OID:
                bson_oid_to_string(bson_iterator_oid(&it), hex_oid);
                fprintf(stderr, "(oid) \"%s\"\n", hex_oid);
                break;
            case BSON_OBJECT:
                fprintf(stderr, "(subobject) {...}\n");
                bson_iterator_subobject(&it, &subobject);
                bson_iterator_init(&it2, &subobject);
                dump_bson_iterator(it2, more_indent);
                break;
            case BSON_ARRAY:
                fprintf(stderr, "(array) [...]\n");
                bson_iterator_subobject(&it, &subobject);
                bson_iterator_init(&it2, &subobject);
                dump_bson_iterator(it2, more_indent);
                break;
            default:
                fprintf(stderr, "(type %d)\n", bson_iterator_type(&it));
                break;
        }
    }
}

- (void) dump
{
    bson_iterator it;
    bson_iterator_init(&it, &bsonValue);
    dump_bson_iterator(it, "");
    fprintf(stderr, "\n");
}

// When an unknown message is received by a RadBSON object, treat it as a call to objectForKey:
- (id) handleUnknownMessage:(id) method withContext:(NSMutableDictionary *) context
{
    Class NuSymbol = NSClassFromString(@"NuSymbol");
    id Nu__null = [NSNull null];
    id cursor = method;
    if (cursor && (cursor != Nu__null)) {
        // if the method is a label, use its value as the key.
        if (NuSymbol && [[cursor car] isKindOfClass:[NuSymbol class]] && ([[cursor car] isLabel])) {
            id result = [self objectForKey:[[cursor car] labelName]];
            return result ? result : Nu__null;
        }
        else {
            id result = [self objectForKey:[[cursor car] evalWithContext:context]];
            return result ? result : Nu__null;
        }
    }
    else {
        return Nu__null;
    }
}

void add_bson_to_object(bson_iterator it, id object, BOOL expandChildren);

id object_for_bson_iterator(bson_iterator it, BOOL expandChildren)
{
    id value = nil;
    
    bson_iterator it2;
    bson subobject;
    char bintype;
    switch(bson_iterator_type(&it)) {
        case BSON_EOO:
            break;
        case BSON_DOUBLE:
            value = [NSNumber numberWithDouble:bson_iterator_double(&it)];
            break;
        case BSON_STRING:
            value = [[NSString alloc]
                     initWithCString:bson_iterator_string(&it) encoding:NSUTF8StringEncoding];
            break;
        case BSON_OBJECT:
            if (expandChildren) {
                value = [NSMutableDictionary dictionary];
                bson_iterator_subobject(&it, &subobject);
                bson_iterator_init(&it2, &subobject);
                add_bson_to_object(it2, value, expandChildren);
            }
            else {
                bson_iterator_subobject(&it, &subobject);
                bson copied_bson;
                bson_copy(&copied_bson, &subobject);
                value = [[RadBSON alloc] initWithBSON:copied_bson];
            }
            break;
        case BSON_ARRAY:
            value = [NSMutableArray array];
            bson_iterator_subobject(&it, &subobject);
            bson_iterator_init(&it2, &subobject);
            add_bson_to_object(it2, value, expandChildren);
            break;
        case BSON_BINDATA:
            bintype = bson_iterator_bin_type(&it);
            value = [NSData
                     dataWithBytes:bson_iterator_bin_data(&it)
                     length:bson_iterator_bin_len(&it)];
            if (bintype == 1) {
                value = [NSKeyedUnarchiver unarchiveObjectWithData:value];
            }
            break;
        case BSON_UNDEFINED:
            break;
        case BSON_OID:
            value = [[RadBSONObjectID alloc]
                     initWithObjectIDPointer:bson_iterator_oid(&it)];
            break;
        case BSON_BOOL:
            value = [NSNumber numberWithBool:bson_iterator_bool(&it)];
            break;
        case BSON_DATE:
            value = [NSDate dateWithTimeIntervalSince1970:(0.001 * bson_iterator_date(&it))];
            break;
        case BSON_NULL:
            value = [NSNull null];
            break;
        case BSON_REGEX:
            break;
        case BSON_CODE:
            break;
        case BSON_SYMBOL:
            break;
        case BSON_CODEWSCOPE:
            break;
        case BSON_INT:
            value = [NSNumber numberWithInt:bson_iterator_int(&it)];
            break;
        case BSON_TIMESTAMP:
            break;
        case BSON_LONG:
            value = [NSNumber numberWithLong:bson_iterator_long(&it)];
            break;
        default:
            break;
    }
    return value;
}

void add_bson_to_object(bson_iterator it, id object, BOOL expandChildren)
{
    while(bson_iterator_next(&it)) {
        NSString *key = [[NSString alloc]
                         initWithCString:bson_iterator_key(&it) encoding:NSUTF8StringEncoding];
        
        id value = object_for_bson_iterator(it, expandChildren);
        if (value) {
            if ([object isKindOfClass:[NSDictionary class]]) {
                [object setObject:value forKey:key];
            }
            else if ([object isKindOfClass:[NSArray class]]) {
                [object addObject:value];
            }
            else {
                fprintf(stderr, "(type %d)\n", bson_iterator_type(&it));
                NSLog(@"we don't know how to add to %@", object);
            }
        }
    }
}

- (NSMutableDictionary *) dictionaryValue
{
    id object = [NSMutableDictionary dictionary];
    bson_iterator it;
    bson_iterator_init(&it, &bsonValue);
    add_bson_to_object(it, object, YES);
    return object;
}

- (NSArray *) allKeys
{
    NSMutableArray *result = [NSMutableArray array];
    bson_iterator it;
    bson_iterator_init(&it, &bsonValue);
    
    while(bson_iterator_next(&it)) {
        NSString *key = [[NSString alloc]
                         initWithCString:bson_iterator_key(&it) encoding:NSUTF8StringEncoding];
        [result addObject:key];
    }
    return result;
}

- (int) count
{
    int count = 0;
    bson_iterator it;
    bson_iterator_init(&it, &bsonValue);
    
    while(bson_iterator_next(&it)) {
        count++;
    }
    return count;
}

- (id) objectForKey:(NSString *) key
{
    bson_iterator it;
    bson_iterator_init(&it, &bsonValue);
    bson_find(&it, &bsonValue, [key cStringUsingEncoding:NSUTF8StringEncoding]);
    id value = object_for_bson_iterator(it, NO);
    return value;
}

- (id) objectForKeyPath:(NSString *) keypath
{
    NSArray *parts = [keypath componentsSeparatedByString:@"."];
    id cursor = self;
    for (int i = 0; i < [parts count]; i++) {
        cursor = [cursor objectForKey:[parts objectAtIndex:i]];
    }
    return cursor;
}

- (id) valueForKey:(NSString *) key {
    return [self objectForKey:key];
}

- (id) valueForKeyPath:(NSString *)keypath {
    return [self objectForKeyPath:keypath];
}

@end

bson *bson_for_object(id object)
{
    bson *b = 0;
    if (!object) {
        object = [NSDictionary dictionary];
    }
    if ([object isKindOfClass:[RadBSON class]]) {
        b = &(((RadBSON *)object)->bsonValue);
    }
    else if ([object isKindOfClass:[NSDictionary class]]) {
        RadBSON *bsonObject = [[RadBSON alloc] initWithDictionary:object];
        //  b = &(bsonObject->bsonValue);
        
        bson_copy(b, &(bsonObject->bsonValue));
        /* puts data in new buffer. NOOP if out==NULL */
    }
    else {
        NSLog(@"unable to convert objects of type %s to BSON (%@).",
              object_getClassName(object), object);
    }
    return b;
}

@implementation RadBSONComparator

+ (RadBSONComparator *) comparatorWithBSONSpecification:(RadBSON *) s
{
    RadBSONComparator *comparator = [[RadBSONComparator alloc] init];
    comparator->specification = s;
    return comparator;
}

- (int) compareDataAtAddress:(void *) aptr withSize:(int) asiz withDataAtAddress:(void *) bptr withSize:(int) bsiz
{
    bson bsonA;
    bsonA.data = aptr;
    //bsonA.owned = NO;
    RadBSON *a = [[RadBSON alloc] initWithBSON:bsonA];
    
    bson bsonB;
    bsonB.data = bptr;
    //bsonB.owned = NO;
    RadBSON *b = [[RadBSON alloc] initWithBSON:bsonB];
    
    bson_iterator it;
    bson_iterator_init(&it, &(specification->bsonValue));
    
    int result = 0;
    while(bson_iterator_next(&it)) {
        NSString *key = [[NSString alloc]
                         initWithCString:bson_iterator_key(&it) encoding:NSUTF8StringEncoding];
        id value = object_for_bson_iterator(it, NO);
        id a_value = [a objectForKey:key];
        id b_value = [b objectForKey:key];
        result = [a_value compare:b_value] * [value intValue];
        if (result != 0)
            break;
    }
    return result;
}

@end

@implementation RadBSONObjectSerialization

+ (id)BSONObjectWithData:(NSData *)data options:(RadBSONReadingOptions)options error:(NSError **)error {

    return [[RadBSON bsonWithData:data] dictionaryValue];
}

+ (NSData *)dataWithBSONObject:(id)object options:(RadBSONWritingOptions)options error:(NSError **)error {
    return [[RadBSON bsonWithDictionary:object] dataRepresentation];
}

@end

// deprecated convenience categories
@implementation NSData (NuBSON)
- (NSMutableDictionary *) BSONValue
{
    return [[RadBSON bsonWithData:self] dictionaryValue];
}

@end

@implementation NSDictionary (RadBSON)
- (NSData *) BSONRepresentation
{
    return [[RadBSON bsonWithDictionary:self] dataRepresentation];
}

@end

