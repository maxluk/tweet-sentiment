globalRowCount = 0;
globalTimerSet = false;

$(function () {

    function numberWithCommas(x) {
        return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

    // Declare a proxy to reference the hub. 
    var twitterHub = $.connection.twitterHub;
    // Create a function that the hub can call to broadcast messages.
    twitterHub.client.updateCounter = function (rowCount) {
        if (globalRowCount < rowCount) {
            globalRowCount = rowCount;

            if (globalTimerSet == false) {
                setTimeout(function () {
                    $('#rowCounter').text(globalRowCount);
                    globalTimerSet = false;
                }, 1000);
                globalTimerSet = true;
            }
        }
    };
    // Start the connection.
    $.connection.hub.start().done(function () {
        //$('#rowCounter').text('connected');
        //$('#sendmessage').click(function () {
        //    // Call the Send method on the hub. 
        //    chat.server.send($('#displayname').val(), $('#message').val());
        //    // Clear text box and reset focus for next comment. 
        //    $('#message').val('').focus();
        //})
    });
});
