//
// Created by mholste on 1/27/2016.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//
// This is a custom handler to pull compressed batches of events from SQS in a JSON tree format.


// Example message
// var message = {
//   "a": {
//    "b": {
//      "c": 1,
//      "d": 2
//     }
//   }
// };
// // outputs as:
// var result = [
//   [ 'a', 'b', 'c', 1 ],
//   [ 'a', 'b', 'd', 2 ]
// ];

(function() {
  "use strict";
  var zlib = require('zlib');
  var root = exports || this;
  var separator = ',';

  function pathfinder(tree, items, path){
    for (var leaf in tree){
      if (typeof(tree[leaf]) === 'object'){
        path.push(leaf);
        if (Object.keys(tree[leaf]).length > 1){
          var branch = path.slice();    
          pathfinder(tree[leaf], items, branch);
        }
        else {
          pathfinder(tree[leaf], items, path);
        }
      }
      else {
        var newpath = path.slice();
        newpath.push(leaf);
        newpath.push(tree[leaf]);
        items.push(newpath.join(separator));
      }
    }
  }

  root.customHandler = function(message, cb) {
    zlib.inflate(message, function(err, buf){
      if (err){
        cb(err, null);
        return;
      }
      message = JSON.parse(buf);
      var ret = [];
      pathfinder(message, ret, []);
      cb(null, ret);
    });
  };

})();

