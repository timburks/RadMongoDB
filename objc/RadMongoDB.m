#import "RadMongoDB.h"
#import "RadBSON.h"

#include "mongo.h"
#include "gridfs.h"

@interface RadBSON ()
{
@public
    bson bsonValue;
}
- (RadBSON *) initWithBSON:(bson) b;
- (id) initWithObjectIDPointer:(const bson_oid_t *) objectIDPointer;
@end

RadBSON *convertToBSONObject(id object) {
    if (!object) {
        object = [NSDictionary dictionary];
    }
    if ([object isKindOfClass:[RadBSON class]]) {
        return object;
    }
    else if ([object isKindOfClass:[NSDictionary class]]) {
        return [[RadBSON alloc] initWithDictionary:object];
    }
    else {
        NSLog(@"unable to convert objects of type %s to BSON (%@).",
              object_getClassName(object), object);
        return nil;
    }
}

@interface RadMongoDB ()
{
    mongo conn;
}
@end

@interface RadMongoDBCursor ()
{
    mongo_cursor *cursor;
}
@end

@implementation RadMongoDBCursor

- (RadMongoDBCursor *) initWithCursor:(mongo_cursor *) c
{
    if (self = [super init]) {
        cursor = c;
    }
    return self;
}

- (mongo_cursor *) cursor
{
    return cursor;
}

- (BOOL) next
{
    return (mongo_cursor_next(cursor) == MONGO_OK);
}

- (bson) current
{
    return cursor->current;
}

- (RadBSON *) currentBSON
{
    bson copied_bson;
    bson_copy(&copied_bson, &(cursor->current));
    return [[RadBSON alloc] initWithBSON:copied_bson];
}

- (NSDictionary *) currentObject
{
    return [[self currentBSON] dictionaryValue];
}

- (void) dealloc
{
    mongo_cursor_destroy(cursor);
}

- (NSMutableArray *) arrayValue
{
    NSMutableArray *result = [NSMutableArray array];
    while([self next]) {
        [result addObject:[self currentObject]];
    }
    return result;
}

- (NSMutableArray *) arrayValueWithLimit:(int) limit
{
    int count = 0;
    NSMutableArray *result = [NSMutableArray array];
    while([self next] && (count < limit)) {
        [result addObject:[self currentObject]];
        count++;
    }
    return result;
}

@end

@implementation RadMongoDB

static BOOL enableUpdateTimestamps = NO;

// "fuzz" the oids with the process id
int oid_fuzz() {
    pid_t pid = getpid();
    return pid;
}

static id oid_synchronizer = nil;
static int oid_counter = 0;

// increment the oids but synchronize to avoid collisions
int oid_inc() {
    @synchronized(oid_synchronizer) {
        return oid_counter++;
    }
}

+ (void) initialize {
    oid_synchronizer = [[NSObject alloc] init];
    bson_set_oid_fuzz(oid_fuzz);
    bson_set_oid_inc(oid_inc);
}

+ (void) setEnableUpdateTimestamps:(BOOL) enable {
	enableUpdateTimestamps = YES;
}

- (int) connectWithOptions:(NSDictionary *) options
{
    mongo_disconnect(&conn);
    id host = options ? [options objectForKey:@"host"] : @"127.0.0.1";
    id port = options ? [options objectForKey:@"port"] : [NSNumber numberWithInt:27017];
    mongo_set_op_timeout(&conn, 1000);
    return mongo_connect(&conn, [host cStringUsingEncoding:NSUTF8StringEncoding], [port integerValue]);
}

- (int) connect
{
    return [self connectWithOptions:nil];
}

- (NSString *) nameForErrorCode:(int) code
{
    switch (code) {
        case MONGO_CONN_SUCCESS:    return @"connection succeeded";
        case MONGO_CONN_NO_SOCKET:  return @"no socket";
        case MONGO_CONN_FAIL:       return @"connection failed";
        case MONGO_CONN_NOT_MASTER: return @"not master";
        default:
            return [NSString stringWithFormat:@"error %d", code];
    }
}

- (void) close {
    mongo_disconnect(&conn);
}

- (void) dealloc {
    mongo_destroy(&conn);
}

- (id) insertObject:(id) insert intoCollection:(NSString *) collection
{
    if (![insert objectForKey:@"_id"]) {
        insert = [insert mutableCopy];
        [insert setObject:[RadBSONObjectID objectID] forKey:@"_id"];
    }
    if (enableUpdateTimestamps) {
        [insert setObject:[NSDate date] forKey:@"_up"];
    }
    RadBSON *bsonObject = convertToBSONObject(insert);
    if (bsonObject) {
        mongo_insert(&conn, [collection cStringUsingEncoding:NSUTF8StringEncoding], &(bsonObject->bsonValue));
        // added for write-safe paranoia
        NSArray *parts = [collection componentsSeparatedByString:@"."];
        if ([parts count]) {
            NSString *database = [parts objectAtIndex:0];
            bson output;
            int lasterror =
            mongo_cmd_get_last_error(&conn,
                                     [database cStringUsingEncoding:NSUTF8StringEncoding],
                                     &output);
            if (lasterror == MONGO_ERROR) {
                NSLog(@"database %@ error %d: %s", database, conn.lasterrcode, conn.lasterrstr);
            }
            // NSLog(@"lasterror %d", lasterror);
        }
        return [insert objectForKey:@"_id"];
    }
    else {
        NSLog(@"incomplete insert: insert must not be nil.");
        return nil;
    }
}

- (BOOL) dropDatabase:(NSString *) database
{
    return mongo_cmd_drop_db(&conn, [database cStringUsingEncoding:NSUTF8StringEncoding]);
}

- (BOOL) dropCollection:(NSString *) collection inDatabase:(NSString *) database
{
    return mongo_cmd_drop_collection(&conn,
                                     [database cStringUsingEncoding:NSUTF8StringEncoding],
                                     [collection cStringUsingEncoding:NSUTF8StringEncoding],
                                     NULL);
}

- (NSMutableDictionary *) findOne:(id) query inCollection:(NSString *) collection
{
    RadBSON *queryBSONObject = convertToBSONObject(query);
    bson bsonResult;
    int result = mongo_find_one(&conn,
                                [collection cStringUsingEncoding:NSUTF8StringEncoding],
                                &(queryBSONObject->bsonValue),
                                0,
                                &bsonResult);
    return (result == MONGO_OK) ?
    [[[RadBSON alloc] initWithBSON:bsonResult] dictionaryValue] : nil;
}

- (int) countWithCondition:(id) condition inCollection:(NSString *) collection inDatabase:(NSString *) database
{
    RadBSON *conditionBSONObject = convertToBSONObject(condition);
    return mongo_count(&conn,
                       [database cStringUsingEncoding:NSUTF8StringEncoding],
                       [collection cStringUsingEncoding:NSUTF8StringEncoding],
                       &(conditionBSONObject->bsonValue));
}

- (BOOL) ensureCollection:(NSString *) collection hasIndex:(NSObject *) key withOptions:(int) options
{
    RadBSON *keyBSONObject = convertToBSONObject(key);
    bson output;
    return mongo_create_index(&conn,
                              [collection cStringUsingEncoding:NSUTF8StringEncoding],
                              &(keyBSONObject->bsonValue),
                              options,
                              &output);
}

- (RadMongoDBCursor *) find:(id) query inCollection:(NSString *) collection
{
    RadBSON *queryBSONObject = convertToBSONObject(query);
    mongo_cursor *cursor = mongo_find(&conn,
                                      [collection cStringUsingEncoding:NSUTF8StringEncoding],
                                      &(queryBSONObject->bsonValue),
                                      0, 0, 0, 0 );
    return [[RadMongoDBCursor alloc] initWithCursor:cursor];
}

- (void) addUser:(NSString *) user withPassword:(NSString *) password forDatabase:(NSString *) database
{
    mongo_cmd_add_user(&conn, [database cStringUsingEncoding:NSUTF8StringEncoding],
                       [user cStringUsingEncoding:NSUTF8StringEncoding],
                       [password cStringUsingEncoding:NSUTF8StringEncoding]);
}

- (BOOL) authenticateUser:(NSString *) user withPassword:(NSString *) password forDatabase:(NSString *) database
{
    return mongo_cmd_authenticate(&conn, [database cStringUsingEncoding:NSUTF8StringEncoding],
                                  [user cStringUsingEncoding:NSUTF8StringEncoding],
                                  [password cStringUsingEncoding:NSUTF8StringEncoding]);
}


- (RadMongoDBCursor *) find:(id) query inCollection:(NSString *) collection returningFields:(id) fields numberToReturn:(int) nToReturn numberToSkip:(int) nToSkip
{
    RadBSON *queryBSONObject = convertToBSONObject(query);
    RadBSON *fieldsBSONObject = convertToBSONObject(fields);
    mongo_cursor *cursor = mongo_find(&conn,
                                      [collection cStringUsingEncoding:NSUTF8StringEncoding],
                                      &(queryBSONObject->bsonValue),
                                      &(fieldsBSONObject->bsonValue),
                                      nToReturn,
                                      nToSkip,
                                      0);
    return [[RadMongoDBCursor alloc] initWithCursor:cursor];
}

- (NSMutableArray *) findArray:(id) query inCollection:(NSString *) collection
{
    RadMongoDBCursor *cursor = [self find:query inCollection:collection];
    return [cursor arrayValue];
}

- (NSMutableArray *) findArray:(id) query inCollection:(NSString *) collection returningFields:(id) fields numberToReturn:(int) nToReturn numberToSkip:(int) nToSkip
{
    RadMongoDBCursor *cursor = [self find:query inCollection:collection returningFields:fields numberToReturn:nToReturn numberToSkip:nToSkip];
    return [cursor arrayValueWithLimit:nToReturn];
}

- (void) updateObject:(id) update inCollection:(NSString *) collection
        withCondition:(id) condition insertIfNecessary:(BOOL) insertIfNecessary updateMultipleEntries:(BOOL) updateMultipleEntries
{
	if (enableUpdateTimestamps) {
    	[update setObject:[NSDate date] forKey:@"_up"];
	}
    RadBSON *updateBSONObject = convertToBSONObject(update);
    RadBSON *conditionBSONObject = convertToBSONObject(condition);
    if (updateBSONObject && conditionBSONObject) {
        mongo_update(&conn, [collection cStringUsingEncoding:NSUTF8StringEncoding],
                     &(conditionBSONObject->bsonValue),
                     &(updateBSONObject->bsonValue),
                     (insertIfNecessary ? MONGO_UPDATE_UPSERT : 0) + (updateMultipleEntries ? MONGO_UPDATE_MULTI : 0));
    }
    else {
        NSLog(@"incomplete update: update and condition must not be nil.");
    }
}

- (void) removeWithCondition:(id) condition fromCollection:(NSString *) collection
{
    RadBSON *conditionBSONObject = convertToBSONObject(condition);
    mongo_remove(&conn,
                 [collection cStringUsingEncoding:NSUTF8StringEncoding],
                 &(conditionBSONObject->bsonValue));
}


- (id) runCommand:(id) command inDatabase:(NSString *) database
{
    RadBSON *commandBSONObject = convertToBSONObject(command);
    bson bsonResult;
    int result = mongo_run_command(&conn,
                                   [database cStringUsingEncoding:NSUTF8StringEncoding],
                                   &(commandBSONObject->bsonValue),
                                   &bsonResult);
    
    return (result == MONGO_OK) ? [[[RadBSON alloc] initWithBSON:bsonResult] dictionaryValue] : nil;
}

- (id) collectionNamesInDatabase:(NSString *) database
{
    NSArray *names = [self findArray:nil inCollection:[database stringByAppendingString:@".system.namespaces"]];
    NSMutableArray *result = [NSMutableArray array];
    for (int i = 0; i < [names count]; i++) {
        id name = [[[names objectAtIndex:i] objectForKey:@"name"]
                   stringByReplacingOccurrencesOfString:[database stringByAppendingString:@"."]
                   withString:@""];
        NSRange match = [name rangeOfString:@".$_id_"];
        if (match.location != NSNotFound) {
            continue;
        }
        match = [name rangeOfString:@"system.indexes"];
        if (match.location != NSNotFound) {
            continue;
        }
        [result addObject:name];
    }
    return result;
}


- (int) writeFile:(NSString *) filePath
     withMIMEType:(NSString *) type
     inCollection:(NSString *) collection
       inDatabase:(NSString *) database
{
    gridfs gfs[1];
    
    gridfs_init(&conn,
                [database cStringUsingEncoding:NSUTF8StringEncoding],
                [collection cStringUsingEncoding:NSUTF8StringEncoding],
                gfs);
    
    int result = gridfs_store_file(gfs,
                                   [filePath cStringUsingEncoding:NSUTF8StringEncoding],
                                   [filePath cStringUsingEncoding:NSUTF8StringEncoding],
                                   [type cStringUsingEncoding:NSUTF8StringEncoding]);
    gridfs_destroy(gfs);
    
    return result;
}

- (int) writeData:(NSData *) data
            named:(NSString *) file
     withMIMEType:(NSString *) type
     inCollection:(NSString *) collection
       inDatabase:(NSString *) database
{
    gridfs gfs[1];
    gridfile gfile[1];
    char buffer[1024];
    NSUInteger i = 0;
    
    gridfs_init(&conn,
                [database cStringUsingEncoding:NSUTF8StringEncoding],
                [collection cStringUsingEncoding:NSUTF8StringEncoding],
                gfs);
    gridfile_writer_init(gfile, gfs, [file cStringUsingEncoding:NSUTF8StringEncoding], [type cStringUsingEncoding:NSUTF8StringEncoding]);
    while(data.length-i > 0) {
        int n = MIN(data.length-i,1024);
        [data getBytes:buffer range:NSMakeRange(i,n)];
        gridfile_write_buffer(gfile, buffer, n);
        i += n;
    }
    int result = gridfile_writer_done(gfile);
    gridfs_destroy(gfs);
    
    return result;
}


- (NSData *) retrieveDataForGridFSFile:(NSString *) filePath
                          inCollection:(NSString *) collection
                            inDatabase:(NSString *) database
{
    gridfs gfs[1];
    gridfile gfile[1];
    gridfs_offset length, chunkLength;
    NSUInteger chunkSize, numChunks;
    
    gridfs_init(&conn,
                [database cStringUsingEncoding:NSUTF8StringEncoding],
                [collection cStringUsingEncoding:NSUTF8StringEncoding],
                gfs);
    if (!gridfs_find_filename(gfs, [filePath cStringUsingEncoding:NSUTF8StringEncoding], gfile)) {
        length = gridfile_get_contentlength(gfile);
        chunkSize = gridfile_get_chunksize(gfile);
        numChunks = gridfile_get_numchunks(gfile);
        NSMutableData *data = [NSMutableData dataWithCapacity:(NSUInteger)length];
        
        char buffer[chunkSize];
        
        for (NSUInteger i = 0; i < numChunks; i++) {
            chunkLength = gridfile_read(gfile, chunkSize, buffer);
            [data appendBytes:buffer length:(NSUInteger)chunkLength];
        }
        gridfs_destroy(gfs);
        
        return data;
    }
    else {
        return nil;
    }
}

- (BOOL) removeFile:(NSString *) filePath
       inCollection:(NSString *) collection
         inDatabase:(NSString *) database
{
    gridfs gfs[1];
    gridfs_init(&conn,
                [database cStringUsingEncoding:NSUTF8StringEncoding],
                [collection cStringUsingEncoding:NSUTF8StringEncoding],
                gfs);
    gridfs_remove_filename(gfs, [filePath cStringUsingEncoding:NSUTF8StringEncoding]);
    gridfs_destroy(gfs);
    return YES;
}

@end
