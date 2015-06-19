var util = require('util');

/**
 * 键值存储，基于Redis
 * @param {RedisClient} client
 */
function kv_redis(client) {
  this.redis = client;
};
kv_redis.prototype.redis = null;
/**
 * 获取指定键的值
 * @param {string} key
 * @param {function (err, val)} cb
 */
kv_redis.prototype.get = function (key, cb) {
  this.redis.get(key, function (err, val) {
    cb(err, val);
  });
};
/**
 * 获取指定组内指定键的值
 * @param {string} group
 * @param {string} key
 * @param {function (err, val)} cb
 */
kv_redis.prototype.getByGroup = function (group, key, cb) {
  this.redis.hget(group, key, function (err, val) {
    cb(err, val);
  });
};
/**
 * 获取指定组内所有的键和值
 * @param {string} group
 * @param {function (err, kvs_obj)} cb
 */
kv_redis.prototype.getGroups = function (group, cb) {
  this.redis.hgetall(group, function (err, rs) {
    cb(err, rs);
  });
};
/**
 * 设置指定键的值
 * @param {string} key
 * @param {string|number} val
 * @param {function (err)} cb
 */
kv_redis.prototype.set = function (key, val, cb) {
  this.redis.set(key, val, function (err) {
    cb(err);
  });
};
/**
 * 设置指定组内指定键的值
 * @param {string} group
 * @param {string} key
 * @param {string|number} val
 * @param {function (err)} cb
 */
kv_redis.prototype.setByGroup = function (group, key, val, cb) {
  this.redis.hset(group, key, val, function (err) {
    cb(err);
  });
};
/**
 * 给指定键的值加上incr
 * @param {string} key
 * @param {number} incr
 * @param {function (err)} cb
 */
kv_redis.prototype.incr = function (key, incr, cb) {
  if (parseInt(incr) == incr) {
    this.redis.incrby(key, incr, function (err) {
      cb(err);
    });
  } else {
    this.redis.incrbyfloat(key, incr, function (err) {
      cb(err);
    });
  }
};
/**
 * 给指定组内指定键的值加上incr
 * @param {string} group
 * @param {string} key
 * @param {number} incr
 * @param {function (err)} cb
 */
kv_redis.prototype.incrByGroup = function (group, key, incr, cb) {
  if (parseInt(incr) == incr) {
    this.redis.hincrby(group, key, incr, function (err) {
      cb(err);
    });
  } else {
    this.redis.hincrbyfloat(group, key, incr, function (err) {
      cb(err);
    });
  }
};
/**
 * 删除指定键
 * @param {string} key
 * @param {function (err)} cb
 */
kv_redis.prototype.delete = function (key, cb) {
  this.redis.del(key, function (err) {
    cb(err);
  });
};
/**
 * 删除指定组内的指定键
 * @param {string} group
 * @param {string} key
 * @param {function (err)} cb
 */
kv_redis.prototype.deleteByGroup = function (group, key, cb) {
  this.redis.hdel(group, key, function (err) {
    cb(err);
  });
};
/**
 * 删除指定组内的所有键
 * @param {string} group
 * @param {function (err)} cb
 */
kv_redis.prototype.deleteGroups = function (group, cb) {
  this.redis.del(group, function (err) {
    cb(err);
  });
};

/**
 * 键值存储，基于Redis
 * @param {RedisClient} client
 * @returns {kv_redis}
 */
module.exports = function (client) {
  return new kv_redis(client);
};
