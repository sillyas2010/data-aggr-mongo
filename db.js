const MongoClient = require('mongodb').MongoClient;
const async = require('async');
const ObjectID = require('mongodb').ObjectID;
const fs = require('fs');

// Database Name
const dbName = 'vfq_prod';
// Connection url
const url = 'mongodb://localhost:27018,localhost:27019';

// Connect using MongoClient
MongoClient.connect(url, {replicaSet: 'rs0', keepAlive: 1, connectTimeoutMS: 30000} , function(err, client) {
    if(!err) {
        var db = client.db(dbName);
        var usersPromise = db.collection('users').aggregate([
            {
                $match: {
                    email: { $regex: "(.*@walmart.com)|(.*@wal-mart.com)|(.*@samsclub.com)|(.*@walmartlabs.com)" }
                }
            },
            {
                $lookup: {
                    from: "userActivity",
                    localField: "_id",
                    foreignField: "userId",
                    as: "userActivity"
                }
            },
            {
                "$addFields": {
                    "userActivity": {
                        "$map": {
                            "input": "$userActivity",
                            "as": "userActivityList",
                            "in": {
                                "activityType": "$$userActivityList.activityType",
                                "date": "$$userActivityList.date"
                            }
                        }
                    }
                }
            },
            {
                $unwind: '$userActivity'
            },
            {
                $sort: {
                    'userActivity.date': 1
                }
            },
            {
                $group: {
                    _id: {
                        _id: '$_id',
                        firstName: '$firstName',
                        lastName: '$lastName',
                        email: '$email'
                    },
                    updates: {
                        $push: '$userActivity'
                    }
                }
            },
            {
                $project: {
                    _id: '$_id._id',
                    firstName: '$_id.firstName',
                    lastName: '$_id.lastName',
                    email: '$_id.email',
                    userActivity: '$updates'
                }
            }
        ], function (err, data) {
            if(!err) {
                var cohortRequests = [];

                data.forEach(function (user) {
                    var id = new ObjectID(user._id);

                    var find = function(callback) {
                        var cohort = db.collection('cohorts').find({
                            members:  { $in: [id] }
                        }, { _id: 0, name: 1});

                        callback(null,cohort);
                    };

                    cohortRequests.push(find);
                });
                
                async.series(cohortRequests, function (err, results) {

                    if(!err) {
                        var newRes = [];

                        results.forEach(function(result) {
                            var func = function (callback) {
                                result.toArray(function (err, docs) {
                                    if(!err) {
                                        callback(null, docs);
                                    } else {
                                        throw err;
                                    }
                                })
                            };

                            newRes.push(func);
                        });

                        async.series(newRes, function (err, res) {
                            if(!err) {
                                var finalUsers = [];

                                data.forEach(function (user,index) {
                                   user.cohorts = res[index];

                                   delete user._id;

                                   finalUsers.push(user);
                                });

                                fs.writeFile('users.json', JSON.stringify(finalUsers, null, 2), 'utf8', function (err, resu) {
                                    if(err) {
                                        throw err;
                                    } else {
                                        console.log('done');
                                        client.close();
                                    }
                                });
                            } else {
                                throw err;
                            }
                        })
                    }
                })
            } else {
                throw err;
            }
        });
    } else {
        if(err) {
            client.close();
            throw err;
        }
    }
});