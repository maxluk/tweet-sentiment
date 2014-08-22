var liveTweetsPos = [];
var liveTweets = [];
var liveTweetsNeg = [];
var map;
var heatmap;
var heatmapNeg;
var heatmapPos;

function initialize() {
    // Initialize the map
    var options = { 
        credentials: "AvFJTZPZv8l3gF8VC3Y7BPBd0r7LKo8dqKG02EAlqg9WAi0M7la6zSIT-HwkMQbx",
        center: new Microsoft.Maps.Location(23.0, 8.0),
        mapTypeId: Microsoft.Maps.MapTypeId.ordnanceSurvey,
        labelOverlay: Microsoft.Maps.LabelOverlay.hidden,
        zoom: 2.5
    };
    var map = new Microsoft.Maps.Map(document.getElementById('map_canvas'), options);

    // Heatmap options for positive, neutral and negative layers

    var heatmapOptions = {
        // Opacity at the centre of each heat point
        intensity: 0.5,

        // Affected radius of each heat point
        radius: 15,

        // Whether the radius is an absolute pixel value or meters
        unit: 'pixels'
    };

    var heatmapPosOptions = {
        // Opacity at the centre of each heat point
        intensity: 0.5,

        // Affected radius of each heat point
        radius: 15,

        // Whether the radius is an absolute pixel value or meters
        unit: 'pixels',

        colourgradient: {
            0.0: 'rgba(0, 255, 255, 0)',
            0.1: 'rgba(0, 255, 255, 1)',
            0.2: 'rgba(0, 255, 191, 1)',
            0.3: 'rgba(0, 255, 127, 1)',
            0.4: 'rgba(0, 255, 63, 1)',
            0.5: 'rgba(0, 127, 0, 1)',
            0.7: 'rgba(0, 159, 0, 1)',
            0.8: 'rgba(0, 191, 0, 1)',
            0.9: 'rgba(0, 223, 0, 1)',
            1.0: 'rgba(0, 255, 0, 1)'
        }
    };

    var heatmapNegOptions = {
        // Opacity at the centre of each heat point
        intensity: 0.5,

        // Affected radius of each heat point
        radius: 15,

        // Whether the radius is an absolute pixel value or meters
        unit: 'pixels',

        colourgradient: {
            0.0: 'rgba(0, 255, 255, 0)',
            0.1: 'rgba(0, 255, 255, 1)',
            0.2: 'rgba(0, 191, 255, 1)',
            0.3: 'rgba(0, 127, 255, 1)',
            0.4: 'rgba(0, 63, 255, 1)',
            0.5: 'rgba(0, 0, 127, 1)',
            0.7: 'rgba(0, 0, 159, 1)',
            0.8: 'rgba(0, 0, 191, 1)',
            0.9: 'rgba(0, 0, 223, 1)',
            1.0: 'rgba(0, 0, 255, 1)'
        }
    };

    // Register and load the Client Side HeatMap Module
    Microsoft.Maps.registerModule("HeatMapModule", "scripts/heatmap.js");
    Microsoft.Maps.loadModule("HeatMapModule", {
        callback: function () {
            // Create heatmap layers for positive, neutral and negative tweets
            heatmapPos = new HeatMapLayer(map, liveTweetsPos, heatmapPosOptions);
            heatmap = new HeatMapLayer(map, liveTweets, heatmapOptions);
            heatmapNeg = new HeatMapLayer(map, liveTweetsNeg, heatmapNegOptions);
        }
    });

    $("#searchbox").val("xbox");
    $("#searchBtn").click(onsearch);
    $("#positiveBtn").click(onPositiveBtn);
    $("#negativeBtn").click(onNegativeBtn);
    $("#neutralBtn").click(onNeutralBtn);
    $("#neutralBtn").button("toggle");
}

function onsearch() {
    var uri = 'api/tweets?query=';
    var query = $('#searchbox').val();
    $.getJSON(uri + query)
        .done(function (data) {
            liveTweetsPos = [];
            liveTweets = [];
            liveTweetsNeg = [];

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
    //Add tweet to the heat map arrays.
    var tweetLocation = new Microsoft.Maps.Location(item.Latitude, item.Longtitude);
    if (item.Sentiment > 0) {
        liveTweetsPos.push(tweetLocation);
    } else if (item.Sentiment < 0) {
        liveTweetsNeg.push(tweetLocation);
    } else {
        liveTweets.push(tweetLocation);
    }
}

function onPositiveBtn() {
    if ($("#neutralBtn").hasClass('active')) {
        $("#neutralBtn").button("toggle");
    }
    if ($("#negativeBtn").hasClass('active')) {
        $("#negativeBtn").button("toggle");
    }

    heatmapPos.SetPoints(liveTweetsPos);
    heatmapPos.Show();
    heatmapNeg.Hide();
    heatmap.Hide();

    $('#statustext').text('Tweets: ' + liveTweetsPos.length + "   " + getPosNegRatio());
}

function onNeutralBtn() {
    if ($("#positiveBtn").hasClass('active')) {
        $("#positiveBtn").button("toggle");
    }
    if ($("#negativeBtn").hasClass('active')) {
        $("#negativeBtn").button("toggle");
    }

    heatmap.SetPoints(liveTweets);
    heatmap.Show();
    heatmapNeg.Hide();
    heatmapPos.Hide();

    $('#statustext').text('Tweets: ' + liveTweets.length + "   " + getPosNegRatio());
}

function onNegativeBtn() {
    if ($("#positiveBtn").hasClass('active')) {
        $("#positiveBtn").button("toggle");
    }
    if ($("#neutralBtn").hasClass('active')) {
        $("#neutralBtn").button("toggle");
    }

    heatmapNeg.SetPoints(liveTweetsNeg);
    heatmapNeg.Show();
    heatmap.Hide();;
    heatmapPos.Hide();;

    $('#statustext').text('Tweets: ' + liveTweetsNeg.length + "\t" + getPosNegRatio());
}

function getPosNegRatio()
{
    if (liveTweetsNeg.length == 0) {
        return "";
    }
    else {
        var ratio = liveTweetsPos.length/liveTweetsNeg.length;
        var str = parseFloat(Math.round(ratio * 10) / 10).toFixed(1);
        return "Positive/Negative Ratio: " + str;
    }
}
