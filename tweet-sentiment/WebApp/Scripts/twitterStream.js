var liveTweetsPos = new google.maps.MVCArray();
var liveTweets = new google.maps.MVCArray();
var liveTweetsNeg = new google.maps.MVCArray();
var map;
var heatmap;
var heatmapNeg;
var heatmapPos;

function initialize() {
    //Setup Google Map
    var myLatlng = new google.maps.LatLng(17.7850, -12.4183);
    var light_grey_style = [{ "featureType": "landscape", "stylers": [{ "saturation": -100 }, { "lightness": 65 }, { "visibility": "on" }] }, { "featureType": "poi", "stylers": [{ "saturation": -100 }, { "lightness": 51 }, { "visibility": "simplified" }] }, { "featureType": "road.highway", "stylers": [{ "saturation": -100 }, { "visibility": "simplified" }] }, { "featureType": "road.arterial", "stylers": [{ "saturation": -100 }, { "lightness": 30 }, { "visibility": "on" }] }, { "featureType": "road.local", "stylers": [{ "saturation": -100 }, { "lightness": 40 }, { "visibility": "on" }] }, { "featureType": "transit", "stylers": [{ "saturation": -100 }, { "visibility": "simplified" }] }, { "featureType": "administrative.province", "stylers": [{ "visibility": "off" }] }, { "featureType": "water", "elementType": "labels", "stylers": [{ "visibility": "on" }, { "lightness": -25 }, { "saturation": -100 }] }, { "featureType": "water", "elementType": "geometry", "stylers": [{ "hue": "#ffff00" }, { "lightness": -25 }, { "saturation": -97 }] }];
    var myOptions = {
        zoom: 3,
        center: myLatlng,
        mapTypeId: google.maps.MapTypeId.ROADMAP,
        mapTypeControl: true,
        mapTypeControlOptions: {
            style: google.maps.MapTypeControlStyle.HORIZONTAL_BAR,
            position: google.maps.ControlPosition.LEFT_BOTTOM
        },
        styles: light_grey_style
    };
    map = new google.maps.Map(document.getElementById("map_canvas"), myOptions);

    //Setup heat map and link to Twitter array we will append data to
    heatmap = new google.maps.visualization.HeatmapLayer({
        data: liveTweets,
        radius: 25
    });

    heatmapNeg = new google.maps.visualization.HeatmapLayer({
        data: liveTweetsNeg,
        radius: 25
    });

    heatmapPos = new google.maps.visualization.HeatmapLayer({
        data: liveTweetsPos,
        radius: 25
    });

    heatmap.setMap(map);

    //var gradientNeg = [
    //  'rgba(0, 255, 255, 0)',
    //  'rgba(0, 255, 255, 1)',
    //  'rgba(0, 191, 255, 1)',
    //  'rgba(0, 127, 255, 1)',
    //  'rgba(0, 63, 255, 1)',
    //  //'rgba(0, 0, 255, 1)',
    //  //'rgba(0, 0, 223, 1)',
    //  'rgba(0, 0, 127, 1)',
    //  'rgba(0, 0, 159, 1)',
    //  'rgba(0, 0, 191, 1)'
    //]
    var gradientNeg = [
        'rgba(0, 255, 255, 0)',
        'rgba(0, 255, 255, 1)',
        'rgba(0, 191, 255, 1)',
        'rgba(0, 127, 255, 1)',
        'rgba(0, 63, 255, 1)',
        'rgba(0, 0, 255, 1)',
        'rgba(0, 0, 223, 1)',
        'rgba(0, 0, 191, 1)',
        'rgba(0, 0, 159, 1)',
        'rgba(0, 0, 127, 1)',
        'rgba(63, 0, 91, 1)',
        'rgba(127, 0, 63, 1)',
        'rgba(191, 0, 31, 1)',
        'rgba(255, 0, 0, 1)'
    ]
    var gradientPos = [
      'rgba(0, 255, 255, 0)',
      'rgba(0, 255, 255, 1)',
      'rgba(0, 255, 191, 1)',
      'rgba(0, 255, 127, 1)',
      'rgba(0, 255, 63, 1)',
      //'rgba(0, 0, 255, 1)',
      //'rgba(0, 0, 223, 1)',
      'rgba(0, 127, 0, 1)',
      'rgba(0, 159, 0, 1)',
      'rgba(0, 191, 0, 1)'
    ]

    heatmapNeg.set('gradient', gradientNeg);

    //Add tweet to the heat map array.
    var tweetLocation = new google.maps.LatLng(40, -120);
    liveTweets.push(tweetLocation);

    //Add tweet to the heat map array.
    var tweetLocationNeg = new google.maps.LatLng(40, -120);
    liveTweetsNeg.push(tweetLocationNeg);

    $("#neutralBtn").button("toggle");

    //Flash a dot onto the map quickly
    //var image = "Content/small-dot-icon.png";
    //var marker = new google.maps.Marker({
    //    position: tweetLocation,
    //    map: map,
    //    icon: image
    //});
    //setTimeout(function () {
    //    marker.setMap(null);
    //}, 600);

    //if (io !== undefined) {
    //    // Storage for WebSocket connections
    //    var socket = io.connect('/');

    //    // This listens on the "twitter-steam" channel and data is 
    //    // received everytime a new tweet is receieved.
    //    socket.on('twitter-stream', function (data) {

    //        //Add tweet to the heat map array.
    //        var tweetLocation = new google.maps.LatLng(data.lng, data.lat);
    //        liveTweets.push(tweetLocation);

    //        //Flash a dot onto the map quickly
    //        var image = "css/small-dot-icon.png";
    //        var marker = new google.maps.Marker({
    //            position: tweetLocation,
    //            map: map,
    //            icon: image
    //        });
    //        setTimeout(function () {
    //            marker.setMap(null);
    //        }, 600);

    //    });

    //    // Listens for a success response from the server to 
    //    // say the connection was successful.
    //    socket.on("connected", function (r) {

    //        //Now that we are connected to the server let's tell 
    //        //the server we are ready to start receiving tweets.
    //        socket.emit("start tweets");
    //    });
    //}
}

function onsearch() {
    var uri = 'api/tweets?query=';
    var query = $('#searchbox').val();
    $.getJSON(uri + query)
        .done(function (data) {
            liveTweetsPos.clear();
            liveTweets.clear();
            liveTweetsNeg.clear();

            // On success, 'data' contains a list of tweets.
            $.each(data, function (key, item) {
                addTweet(item);
            });

            if (!$("#neutralBtn").hasClass('active')) {
                $("#neutralBtn").button("toggle");
            }
            onNeutralBtn();
        })
        .fail(function (jqXHR, textStatus, err) {
            $('#statustext').text('Error: ' + err);
        });
}

function addTweet(item) {
    //Add tweet to the heat map array.
    var tweetLocation = new google.maps.LatLng(item.Latitude, item.Longtitude);
    if (item.Sentiment > 0) {
        liveTweetsPos.push(tweetLocation);
    } else if (item.Sentiment < 0) {
        liveTweetsNeg.push(tweetLocation);
    } else {
        liveTweets.push(tweetLocation);
    }
        //Flash a dot onto the map quickly
        //var image = "Content/small-dot-icon.png";
        //if (item.Sentiment < 0) {
        //    var marker = new google.maps.Marker({
        //        position: tweetLocation,
        //        map: map,
        //        icon: image
        //    });
        //}
        //setTimeout(function () {
        //    marker.setMap(null);
        //}, 600);
}

function onPositiveBtn() {
    if ($("#neutralBtn").hasClass('active')) {
        $("#neutralBtn").button("toggle");
    }
    if ($("#negativeBtn").hasClass('active')) {
        $("#negativeBtn").button("toggle");
    }

    heatmapPos.setMap(map);
    heatmapNeg.setMap(null);
    heatmap.setMap(null);

    $('#statustext').text('Tweets: ' + liveTweetsPos.length);
}

function onNeutralBtn() {
    if ($("#positiveBtn").hasClass('active')) {
        $("#positiveBtn").button("toggle");
    }
    if ($("#negativeBtn").hasClass('active')) {
        $("#negativeBtn").button("toggle");
    }

    heatmap.setMap(map);
    heatmapNeg.setMap(null);
    heatmapPos.setMap(null);

    $('#statustext').text('Tweets: ' + liveTweets.length);
}

function onNegativeBtn() {
    if ($("#positiveBtn").hasClass('active')) {
        $("#positiveBtn").button("toggle");
    }
    if ($("#neutralBtn").hasClass('active')) {
        $("#neutralBtn").button("toggle");
    }

    heatmapNeg.setMap(map);
    heatmap.setMap(null);
    heatmapPos.setMap(null);

    $('#statustext').text('Tweets: ' + liveTweetsNeg.length);
}
