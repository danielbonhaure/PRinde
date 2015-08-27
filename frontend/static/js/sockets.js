/**
 * Created by federico on 08/08/15.
 */

$(document).ready(function () {
    var namespace = '/observers';
    var socket = io.connect('http://localhost:5000' + namespace)

    socket.on('connect', function () {
        $('#link-status').removeClass('text-warning').addClass('connected');
        $('#log').empty();

        socket.emit('connected')
    });

    socket.on('disconnect', function () {
        $('#link-status').removeClass('connected').addClass('text-warning');
    });

    socket.on('logs', function (msg) {
        lines = msg.split("\n");

        var $log = $('#log');

        $.each(lines, function (idx, line) {
            if(line.trim().length == 0) return;

            var log_level = 'bs-callout bs-callout-default';

            if (line.indexOf('INFO') > -1) {
                log_level = 'text-info bs-callout bs-callout-info'
            } else if (line.indexOf('WARN') > -1) {
                log_level = 'text-warning bs-callout bs-callout-warning'
            } else if (line.indexOf('ERR') > -1) {
                log_level = 'text-danger bs-callout bs-callout-danger'
            }
            $log.append('<div class="' +
                log_level + '">' + line + '</div>')
        });

        $log.scrollTop($log[0].scrollHeight);
    });

    var log_original_size = $('#log').height();

    $('#resize-log').click(function() {
        var $log = $('#log');
        var $this = $(this).children().first();

        console.log($this.className);

        //if($this.className.indexOf('glyphicon-resize-full') > -1) {
        if($this.hasClass('glyphicon-resize-full')) {
            $log.height($(document).height() - 135);
            $this.removeClass('glyphicon-resize-full').addClass('glyphicon-resize-small')
        } else {
            $log.height(log_original_size);
            $this.removeClass('glyphicon-resize-small').addClass('glyphicon-resize-full')
        }
    })
});