// Copyright 2018, Abhi Nayar, Yale University
// CS + Econ, Class of 2018. All packages used belong
// to their respective owners and are used in accordance
// with their respective licences. This project is
// academic and non-commercial.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This project was made to satisfy the requirements of CPSC 490, Senior Project
// in the Spring of 2018. It was advised by Prof. Guy Wolf (https://math.yale.edu/people/guy-wolf)
// in tandem with Prof. James Aspnes (http://www.cs.yale.edu/homes/aspnes/)

// This is the backend code for Birth of a Revolution -- A Global Model For Forecasting Political Instability (BoR)
// BoR is an attempt to quantify levels of social unrest around the world using a combination of
// socioeconomic indicators and static models combined with real-time social media data obtained via sentiment analysis
// on Twitter and Tumblr streams. The front-end is a WebGL based visualization allowing anyone to view realtime
// microblog data tagged with sentiment, as well as explore time-series data for 150 countries and their respective
// unrest timelines. For any questions, please contact the author at abhishek.nayar[at]yale[dot]edu

//
//
// === BEGIN SERVER SIDE CODE ===
//

//
//
// === Import dependencies
const express = require('express'),
http = require('http'),
socketio = require('socket.io'),
path = require('path'),
bodyParser = require('body-parser'),
port = process.env.PORT || 3000,
app = express();

//
//
// === Load local environment vars from .env file
require('dotenv').config({ path : './config/.env' })

//
//
// === Setup the server & vars.
const server = http.createServer(app),
io = socketio(server);

// Some placeholder variables so we can use these in other functions
// Useful for playing and pausing the stream to use with the search function
let socketConnection;

//
//
// === Setup the app
app.use(bodyParser.json())

//
//
// === Setup Firebase
const firebaseAdmin = require('firebase-admin');
let serviceAccount = require('./config/b-o-r-490-firebase-admin.json');
let firebaseApp = firebaseAdmin.initializeApp({
  credential : firebaseAdmin.credential.cert(serviceAccount),
  databaseURL : process.env.FIREBASE_DATABASE_URL
});
// Un-prefix the variable here (no const, let, etc.)
// Make this globally available in the routes, etc.
// NOTE: This is NOT the best practice way to handle this. It's quick and easy.
database = firebaseApp.database();

//
//
// === Setup Google NLP
const GoogleLanguageAPI = require('@google-cloud/language');
GoogleLanguageClient = new GoogleLanguageAPI.LanguageServiceClient({
  keyFilename: './config/b-o-r-490-service-account.json'
});

//
//
// === Setup BigQuery
const BigQuery = require('@google-cloud/bigquery');
BigQueryClient = new BigQuery({
  projectId : 'b-o-r-490'
})

//
//
// === Setup Google Maps Client
const googleMapsClient = require('@google/maps').createClient({
  key: process.env.GOOGLE_MAPS_KEY
});

//
//
// === Setup Twitter
const TwitterStream = require('twitter-stream-api'),
twitterKeys = {
  consumer_key : process.env.TWITTER_CONS_KEY,
  consumer_secret : process.env.TWITTER_CONS_SECRET,
  token : process.env.TWITTER_ACCESS_TOKEN,
  token_secret : process.env.TWITTER_ACCESS_SECRET
};

// Again, make these globally available
Twitter = new TwitterStream(twitterKeys, true);

//
//
// === CORE APP LOGIC ===
//
//

// Set the searchTerm to app locals here
// We will default to the usual trackedWords array (located in ./config folder)
// But we want a way to update the Twitter stream word on the fly
// Setup the word tracking and the potential regexp parsing

// Why regexp parsing? When we view on a country by country basis, we want
// to zoom in onto one country and pull in tweets via a bounding box of latLong coords.
// Unfortunately the Twitter API does not allow for AND'ed calls -- You will get both
// the FULL stream of a countries tweets AS WELL AS any global tweets matching your keyword

// To get around this, we simply pull in all tweets within the bounding box (well all tweets
// returned by the public API) and then parse it through a naive regex check. We can safely join
// on | because we manually create the trackedWords array and know that no JS escapable chars
// exist within said array. Cheers!
let trackedWords = require('./config/trackedWords'),
joinedKeywords = new RegExp(trackedWords.join("|"));

// Set the locals... these can change on the fly with incoming POST requests
app.locals.searchTerms = trackedWords;
// Testing...
app.locals.searchTerms = 'happy'

/**
  * @desc Starts the twitter stream while closing any open ones
  * @param none
  * @return calls Twitter stream API
*/
const startTwitterStream = () => {
  // For debugging
  console.log(Twitter);

  // Check if we already have an open stream
  // Having multiple streams open at the same time can lead to errors
  // Only open a new string is there isn't a readable already listening
  // TODO: Need to brush up on Node streams
  if (!Twitter._readableState.readableListening) {
    console.log("Starting Twitter Stream")

    // @param track == the keywords being tracked, stall_warnings == throw warning if the carriage return comes in
    // @optional_params == locations, locations == bounding box of latLong coords to stream twits from
    Twitter.stream('statuses/filter', {
      track : app.locals.searchTerms,
      stall_warnings: true,
      //locations : '-171.791110603, 18.91619, -66.96466, 71.3577635769'
    })
  } else {
    // There is already a Twitter stream, restart the b
    console.log("Tried to start twitter stream, but it already exists\nClosing current stream and opening a new one.");
    restartTwitterStream();
  }
}

/**
  * @desc Closes any existing Twitter stream and restarts it. Is a pure function.
  * @param none
  * @return calls startTwitterStream
*/
const restartTwitterStream = () => {
  // First close any stream that may be open
  // This ideally fails quietly if there is no stream open, though that is a TODO to check
  Twitter.close();
  // Start a new stream
  startTwitterStream();
  return;
}

/**
  * @desc Handles the incoming tweet
  * @param tweet (object), reference = https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
  * @return calls emitTweet
*/
async function handleTweet(tweet) {
  //console.log('Received a tweet!')

  // First we run the NLP algo on the tweet text
  // Only continue if we have a valid tweet object
  // We also want to selectively look at tweets with Geo data
  // Reason? We have enough volume incoming with our keywords list to discriminate
  // HOWEVER TODO == We need to have a flag for this so that we can stream all of a keyword that is more sparse
  if (tweet && tweet.text !== undefined) {
    if (tweet.coordinates || tweet.place) {
      // @sentiment = { sentiment: ___, magnitude: __ }
      //let sentiment = await checkTweetSentiment(tweet.text);
      let sentiment = {  score : 0.5, magnitude : 0 };

      // Check the Promise didn't throw an error
      if (sentiment.sentiment !== 'error') {
        //if (tweet.coordinates) console.log(tweet.coordinates.coordinates)
        //else if (tweet.place) console.log(tweet.place.bounding_box.coordinates)

        // We got the tweet + sentiment
        // Let's emit it via socketIO to client(s)
        emitTweet(tweet, sentiment)
      } else {
        // An error occured getting the sentiment
        console.log("An error occured", sentiment.error);
      }
    } else {
      // TODO
      //console.log('NO LOCATION', tweet.text);
      //let sentiment = await checkTweetSentiment(tweet.text);
      //console.log(sentiment.score);
    }
  }
}

/**
  * @desc Runs the Google Language Parsing
  * @param tweetText (text from incoming tweet)
  * @return sentiment (object containing sentiment score + magnitude/confidence)
  *         { sentiment : ____, magnitude: ____ }
*/
async function checkTweetSentiment(tweetText) {
  // Setup the ingestion objects for the Google Language API
  const document = {
    content: JSON.stringify(tweetText),
    type: 'PLAIN_TEXT',
  },
  features = {
    //"extractSyntax" : true, //TODO
    //"extractEntities" : true, //TODO
    "extractDocumentSentiment" : true
  }

  // Promise the Language API call
  return new Promise((resolve, reject) => {
    GoogleLanguageClient.analyzeSentiment({
      "document" : document,
      "features" : features
    }).then(results => {
      // @sentiment = { score : score, magnitude : magnitude]
      const sentiment = results[0].documentSentiment;

      if (sentiment) {
        // Resolve the promise positively
        resolve(sentiment)
      } else {
        resolve({ sentiment : "error", error : "The sentiment object could not be validated : " + JSON.stringify(sentiment) })
      }
    }).catch(err => {
      // Something went wrong with the API
      // MOST COMMON: Unsupported language
      // Resolve, but with error
      resolve({ sentiment : "error", error : err })
    });
  });
}

/**
  * @desc Emits tweet via socketio
  * @param tweet | sentiment, tweet = tweet object, sentiment = sentiment object
  * @return none, streams tweet data via socketio
*/
async function emitTweet(tweet, sentiment) {
  // Testing
  console.log(tweet.text, '\nSentiment:', sentiment.score)

  // We will fill this with the geo of the tweet
  let coords,
  geoString = '';

  // Check and average the bounding box coords (with tweet.place) or just set coords = tweet.coordinates
  // Check twitter API for details on these object population
  if (tweet.place) {
    console.log("tweet.place found");
    if (tweet.place.full_name && tweet.place.country) {
      geoString = tweet.place.full_name + ', ' + tweet.place.country;
      console.log("Constructed tweet.place string: ", geoString);
      // let googleCoords = await getLatLongFromCity(geoString)
      //
      // if (googleCoords !== 'error') {
      //   coords = [ googleCoords.lat, googleCoords.lng]
      // } else {
      //   console.log("Did not receive geotag from google")
      //   if (tweet.place.bounding_box.coordinates) {
      //     console.log("Averaging bounding box instead")
      //     coords = averageCoords(tweet.place.bounding_box.coordinates).reverse();
      //   } else {
      //     console.log("No bounding box found, checking tweet.coordinates")
      //     if (tweet.coordinates) coords = tweet.coordinates
      //     else coords = null;
      //   }
      // }

      coords = averageCoords(tweet.place.bounding_box.coordinates)
    } else {
      console.log("Could not create tweet.place string, averaging boundign box instead");
      if (tweet.place.bounding_box.coordinates) {
        coords = averageCoords(tweet.place.bounding_box.coordinates)
      } else {
        console.log("Could not find tweet.place bounding box, fuck it setting null")
        coords = null;
      }
    }
  }
  else if (tweet.coordinates) {
    console.log("No tweet.place, got tweet.coordinates instead")
    coords = tweet.coordinates
  }
  else {
    console.log("No tweet location ,setting null");
    coords = null
  }

  let countryCode = getCountryCode(tweet.place);

  // Create the object to be emitted
  console.log("Creating emit object")
  let emittedObj = {
    tweet : tweet.text,
    coords : coords,
    geo : geoString || 'none',
    countryCode : countryCode,
    sentiment : sentiment
  }

  console.log("Emitting object" , emittedObj)
  // Emit the tweet via socketio
  socketConnection.emit('tweet', emittedObj)
}

//
//
// === Setup Socket.io
/**
  * @desc Sets up socketio stream
  * @param none, is a stream
  * @return none, populates socketConnection and calls startTwitterStream
*/
io.on('connection', socket => {
  // Poulate socketConnection so we can use it later
  // Used in emitTweet
  socketConnection = socket;

  // Start the twitter stream
  startTwitterStream();

  // TODO: Figure out why the fuck these dont work
  socket.on('connection', () => console.log("Client CONnected"));
  socket.on('disconnect', () => console.log("Client DISconnected"));
  socket.on('error', (err) => console.log(err));
})

// === Setting up less common Twitter stream events here
/**
  * @desc Handles the event that a valid tweet is received
  * @param stream
  * @return none, calls handleTweet or logInvalidTweet
*/
Twitter.on('data', tweet => {
  (tweet && tweet.text !== undefined) ? handleTweet(tweet) : logInvalidTweet(tweet);
});

/**
  * @desc Handles the keep alive event when no tweets are received in > 30 secs. Is a carriage return \r\n
  *       See Twitter API for details.
  * @param stream
  * @return none, logs to console.
*/
Twitter.on('data keep-alive', () => {
  console.log('Twitter DATA KEEP ALIVE');
});

// These stream events result in error cascades, so we need to handle them
// They also are best handled by killing and restarting the current stream
Twitter.on('connection aborted', () => {
  console.log('Twitter connection ABORTED')
  // Restart stream
  setTimeout(() => {
    //restartTwitterStream();
  }, 2000)
})
Twitter.on('connection error network', (err) => {
  console.log('Twitter network error occurred =>', err)
  // Restart stream
  //restartTwitterStream();
})
Twitter.on('connection error unknown', (error) => {
  console.log('connection error unknown', error);
  // Restart stream
  //restartTwitterStream();
});
Twitter.on('data error', err => {
  //console.log('Tweet data ERROR: ', err);
  // Restart stream
  setTimeout(() => {
    //restartTwitterStream();
  }, 2000)
});

// Subsequent events are not common enoiugh to need an explanation.
// See Twitter API as well as @link = npmjs.com/package/twitter-stream-api for details
Twitter.on('connection success', (uri) => {
  console.log('Twitter connection established =>', uri)
})
Twitter.on('connection error stall', () => {
  console.log('Twitter connection error stall');
});
Twitter.on('connection error http', (httpStatusCode) => {
  console.log('Twitter connection error http', httpStatusCode);
});
Twitter.on('connection rate limit', (httpStatusCode) => {
  console.log('Twitter connection rate limit', httpStatusCode);
});

//
//
// === Helper functions
/**
  * @desc Averages bounding box of latLong coordinates to single center
  * @param coordinates (bounding box) from tweet object
  * @return latLong coordinate of bounding box center
*/
const averageCoords = (coordinates) => {
  var n = 0, lon = 0.0, lat = 0.0;
  coordinates.forEach(function(latLongs) {
    latLongs.forEach(function(latLong) {
      lon += latLong[0];
      lat += latLong[1];
      n += 1;
    })
  });
  return [lat / n, lon / n];
}

/**
  * @desc Logs invalid tweets to the console, most often keep alives
  * @param invalid Tweet Data
  * @return none, logging function
*/
const logInvalidTweet = (data) => {
  console.log("An invalid tweet was received: ", data);
}

/**
  * @desc Calls Google maps api on the city string and returns latlong
  * @param cityString
  * @return latLong coordinates of city
*/
async function getLatLongFromCity(cityString) {
  return new Promise(function(resolve, reject) {
    googleMapsClient.geocode({
      address: cityString
    }, function(err, response) {
      //console.log("Geotag results", err, response);

      if (!err && checkGeotagResponse(response)) {
        console.log("Received address")
        resolve(response.json.results[0].geometry.location);
      } else {
        console.log("Geotag error", err);
        resolve("error");
      }
    });
  });
}

/**
  * @desc Checks if the geotag returned a good enough location to use
  * @param geotagResponse
  * @return truthy value of whether the goetagResponse has latLong coords or not
*/
const checkGeotagResponse = (response) => {
  if (response.json) {
    if (response.json.results) {
      if (response.json.results[0]) {
        if (response.json.results[0].geometry) {
          if (response.json.results[0].geometry.location) {
            return true;
          } else {
            console.log("Failed 5", response.json.results[0].geometry)
            return false
          }
        } else {
          console.log("Failed 4", response.json.results[0])
          return false;
        }
      } else {
        console.log("Failed 3", response.json.results)
        return false;
      }
    } else {
      console.log("Failed 2", response.json)
      return false;
    }
  } else {
    console.log("Failed 1", response)
    return false;
  }
}

/**
  * @desc Gets country code from tweet.place
  * @return country code OR false
*/
const getCountryCode = (place) => {
  if (place.country_code) {
    return place.country_code
  } else {
    return '-1'
  }
}

//
//
// === Start the server
server.listen(port, err => {
  if (err) throw(err);
  console.log('Server listening on: ', port)
})
