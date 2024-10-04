var checkout = {};

$(document).ready(function() {
  var $messages = $('.messages-content'),
    d, h, m,
    i = 0;

  $(window).load(function() {
    $messages.mCustomScrollbar();
    insertResponseMessage('Hi there, I\'m your personal Concierge. How can I help?');
  });

  function updateScrollbar() {
    $messages.mCustomScrollbar("update").mCustomScrollbar('scrollTo', 'bottom', {
      scrollInertia: 10,
      timeout: 0
    });
  }

  function setDate() {
    d = new Date()
    if (m != d.getMinutes()) {
      m = d.getMinutes();
      $('<div class="timestamp">' + d.getHours() + ':' + m + '</div>').appendTo($('.message:last'));
    }
  }

  function callChatbotApi(msg) {
    var apigClient = apigClientFactory.newClient();  // Initialize the API Gateway client
    var params = {};  // No query parameters needed for a POST request
    var body = {
      message: msg   // The message input from the user
    };
    var additionalParams = {};  // No additional headers or parameters are required
  
    // Make the POST request to the chatbot API via API Gateway
    return apigClient.chatbotPost(params, body, additionalParams)
      .then(function(response) {
        console.log("API Gateway response:", response);  // Log the response for debugging
        return response;  // Return the full response object
      })
      .catch(function(error) {
        console.error("Error in API call:", error);  // Log any errors
        throw error;  // Re-throw the error so it can be caught and handled later
      });
  }
  
  function insertMessage() {
    var msg = $('.message-input').val();
    if ($.trim(msg) === '') {
      return false;
    }
    
    $('<div class="message message-personal">' + msg + '</div>').appendTo($('.mCSB_container')).addClass('new');
    setDate();
    $('.message-input').val(null);
    updateScrollbar();
  
    // Call the modified function that uses API Gateway SDK
    callChatbotApi(msg)
      .then((response) => {
        var data = response.data;
  
        if (data.messages && data.messages.length > 0) {
          console.log('received ' + data.messages.length + ' messages');
          var messages = data.messages;
  
          for (var message of messages) {
            if (message.type === 'unstructured') {
              insertResponseMessage(message.unstructured.text);
            } else if (message.type === 'structured' && message.structured.type === 'product') {
              var html = '';
  
              insertResponseMessage(message.structured.text);
  
              setTimeout(function() {
                html = '<img src="' + message.structured.payload.imageUrl + '" width="200" height="240" class="thumbnail" /><b>' +
                  message.structured.payload.name + '<br>$' +
                  message.structured.payload.price +
                  '</b><br><a href="#" onclick="' + message.structured.payload.clickAction + '()">' +
                  message.structured.payload.buttonLabel + '</a>';
                insertResponseMessage(html);
              }, 1100);
            } else {
              console.log('not implemented');
            }
          }
        } else {
          insertResponseMessage('Oops, something went wrong. Please try again.');
        }
      })
      .catch((error) => {
        console.log('an error occurred', error);
        insertResponseMessage('Oops, something went wrong. Please try again.');
      });
  }
  

  $('.message-submit').click(function() {
    insertMessage();
  });

  $(window).on('keydown', function(e) {
    if (e.which == 13) {
      insertMessage();
      return false;
    }
  })

  function insertResponseMessage(content) {
    $('<div class="message loading new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure><span></span></div>').appendTo($('.mCSB_container'));
    updateScrollbar();

    setTimeout(function() {
      $('.message.loading').remove();
      $('<div class="message new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure>' + content + '</div>').appendTo($('.mCSB_container')).addClass('new');
      setDate();
      updateScrollbar();
      i++;
    }, 500);
  }

});
