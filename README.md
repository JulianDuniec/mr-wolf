[![build status](https://secure.travis-ci.org/JulianDuniec/mr-wolf.png)](http://travis-ci.org/JulianDuniec/mr-wolf)
 
mr-wolf
=======

Distributed work-queue for nodejs

## Installation
  $ npm install mr-wolf
  
## Usage

### Create a job

Place your work in the default /jobs-directory. Note that you can change the location of this directory using

```js

mrWolf.setJobDirectory('./some/other/place');

```

example: /jobs/sendEmail.js
```js

module.exports = {
  //A job must implement the receive-method that takes two parameters
  //meta : the meta-data sent to the job
  //callback : a callback with one parameter : error (if any)
  receive : function(meta, callback) {
    sendEmail(meta.recipient, meta.title, meta.body, function(err) {
      callback(err);
    });
  }
};

```

### Enqueue a work-item

```js

var mrWolf = require('mr-wolf');
//Push takes two arguments, 
//the job-name (should be equivalent to the fileName mentioned in create a job)
//and the meta-data, which will also be supplied to the job.
mrWolf.enqueue('sendEmail', {
  recipient : "mr.recipient@coolmail.com",
  title : 'Hello mr!',
  body : 'Its a fifteen minute drive, I\'ll be there in five'
});

```

### Start working

Note that starting mr-wolf on many servers lets them share the work-load and process the queue faster.  

Here's how you get start it up:
```js

mrWolf.start({
  //Add some listeners (optional)
  onError : function(job, error) { },
  onStart : function(job) { },
  onComplete : function(job) { },
  onEnqueue : function(job) { }
});
```