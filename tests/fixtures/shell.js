function check(condition, message) {
  if (!condition) throw new Error(message);
}
const items = db.getCollection('shell_items');
items.deleteMany({});
const id = ObjectId('507f1f77bcf86cd799439011');
const inserted = items.insertOne({
  _id: id, n: NumberLong('9223372036854775807'), min: -9223372036854775808n,
  date: ISODate('2026-01-02T03:04:05Z'), double: NumberDouble(1), negativeZero: NumberDouble(-0),
  decimal: NumberDecimal('123.4567890123456789012345678901234'),
  binary: BinData(128, 'AP8D'), literal: {$oid: 'ordinary object'}
});
check(String(inserted.insertedId) === String(id), 'inserted identity');
const document = items.findOne({_id: id});
check(String(document.n) === '9223372036854775807', 'int64 maximum');
check(String(document.min) === '-9223372036854775808', 'int64 minimum');
check(document.date.toISOString() === '2026-01-02T03:04:05.000Z', 'date');
check(Object.is(document.negativeZero.valueOf(), -0), 'negative zero');
check(document.literal.$oid === 'ordinary object', 'literal Extended JSON shape');
document._id = 'copied';
items.insertOne(document);
check(items.findOne({_id:'copied'}).n.valueOf() === 9223372036854775807n, 'reuse typed returned values');
let duplicate = false;
try { items.insertOne({_id:id}); } catch (error) { duplicate = error.code === 11000; }
check(duplicate, 'duplicate write errors must throw');
check(items.find().length === 2, 'recovery after database error');
for (const invalid of [null, 'abc', [], 123]) {
  let rejected = false;
  try { items.insertOne(invalid); } catch (_) { rejected = true; }
  check(rejected, 'invalid insert must fail');
}
for (const method of ['deleteOne', 'deleteMany']) {
  let rejected = false;
  try { items[method](); } catch (_) { rejected = true; }
  check(rejected, 'missing delete filter must fail');
}
check(items.find().length === 2, 'invalid operations must not change data');
items.deleteMany({});
items.insertMany(Array.from({length:250}, (_,i) => ({_id:i, n:i})));
check(items.find({}, {sort:{_id:1}, batchSize:7}).length === 250, 'all cursor batches');
check(items.find({}, {sort:{_id:-1}, limit:2})[0]._id === 249, 'find options');
check(items.updateOne({_id:0}, {$set:{mark:true}}).matchedCount == 1, 'single update');
check(items.updateMany({_id:{$lt:3}}, {$inc:{n:1}}).modifiedCount == 3, 'multi update');
check(items.replaceOne({_id:249}, {n:10}).modifiedCount == 1, 'replacement');
check(items.aggregate([{$match:{_id:{$lt:3}}}, {$group:{_id:null, total:{$sum:'$n'}}}])[0].total == 6, 'aggregate');
check(items.deleteOne({_id:{$lt:3}}).deletedCount == 1, 'single delete');
check(items.deleteMany({_id:{$lt:3}}).deletedCount == 2, 'multi delete');
check(db.listCollections().some(c => c.name === 'shell_items'), 'list collections');
({ok:true, remaining:items.find().length})
