/*
 * (c) 2003-2015 MuleSoft, Inc. This software is protected under international copyright
 * law. All use of this software is subject to MuleSoft's Master Subscription Agreement
 * (or other master license agreement) separately entered into in writing between you and
 * MuleSoft. If such an agreement is not in place, you may not use the software.
 */
$(".dropdown").on("click", function(e){
	  e.preventDefault();

	  if($(this).hasClass("open")) {
	    $(this).removeClass("open");
	    $(this).children("ul").slideUp("fast");
	  } else {
  		var list = document.getElementsByClassName("dropdown");;

		for (var i=0, item; item = list[i]; i++) {
		  	  if($(item).hasClass("open")) {
			    $(item).removeClass("open");
			    $(item).children("ul").slideUp("fast");
			  }
		}

	    $(this).addClass("open");
	    $(this).children("ul").slideDown("fast");
	  }
});

$("a").on("click", function(e){
	  e.preventDefault();
	  	var demoLink = this.getAttribute("href");
	  	window.location.href = demoLink;
});
