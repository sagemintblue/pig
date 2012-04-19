// globals
var backendBaseUrl = "http://localhost:8080";
var lastEventId = 0

/**
 * Displays an error message.
 */
function displayError(msg) {
  // TODO(Andy Schlaikjer): Display error, pause event polling
}

/**
 * Retrieves snapshot of current DAG of scopes from back end.
 */
function getScopeGraph() {
  d3.json(backendBaseUrl + "/dag", function(d) {
    // test for error
    if (d == null) {
      displayError("Failed to retrieve scope graph")
      return
    }

    // TODO(Andy Schlaikjer): Replace existing scope graph data with new data and update viz
  });
}


// TODO(Andy Schlaikjer): update dag state based on event and trigger viz updates
function handleJobStartedEvent(event) {
  alert("Job started: " + event.eventData.jobId);
};
function handleJobCompleteEvent(event) {
  alert("Job complete: " + event.eventData.jobId);
};
function handleJobFailedEvent(event) {
  alert("Job failed: " + event.eventData.jobId);
};
function handleJobProgressEvent(event) {
  alert("Job progress: " + event.eventData.jobId);
};
function handleScriptProgressEvent(event) {
  d3.select('#scriptDialog')
      .append("svg:text")
      .text('script progress: ' + event.eventData.scriptProgress + '%');
  //$('scriptDialog').innerHTML = 'script progress: ' + event.eventData.scriptProgress + '%';
   alert("Script progress: " + event.eventData.scriptProgress);
};

var lastProcessedEventId = -1;
/** 
 * Polls back end for new events.
 */
function pollEvents() {
  d3.json("pig-events.json?lastEventId=" + lastProcessedEventId, function(events) {
    // test for error
    if (events == null) {
      displayError("No events found")
      return
    }
    var eventsHandledCount = 0;
    events.forEach(function(event) {
        var eventId = event.eventId;
        if (eventId <= lastProcessedEventId || eventsHandledCount > 0) {
            return;
        }
        var eventType = event.eventType;
        if(eventType == "JOB_STARTED") {
            handleJobStartedEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        } else if(eventType == "JOB_PROGRESS") {
            handleJobProgressEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        } else if(eventType == "JOB_COMPLETE") {
            handleJobCompleteEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        } else if(eventType == "JOB_FAILED") {
            handleJobFailedEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        } else if(eventType == "SCRIPT_PROGRESS") {
            handleScriptProgressEvent(event);
            lastProcessedEventId = eventId;
            eventsHandledCount++;
        }
    });
  });
}

// kick off event poller and keep track of interval id so we can pause polling if needed
//var pollEventsIntervalId = setInterval(pollEvents, 10000);

// storage for job data
var jobs;

// set up viz params
var r1 = 600 / 2;
var r0 = r1 - 120;

// define color palette
var fill = d3.scale.category20b();

// job dependencies are visualized by chords
var chord = d3.layout.chord()
  .padding(0.04)
  .sortSubgroups(d3.descending)
  .sortChords(d3.descending);

function groupStartAngle(d) {
  return (2 * Math.PI / jobs.length) * d.index;
}

function groupEndAngle(d) {
  var r = 2 * Math.PI / jobs.length;
  return r * (d.index + 1) - r * 0.1;
}

// jobs themselves are arc segments around the edge of the chord diagram
var arc = d3.svg.arc()
  .innerRadius(r0)
  .outerRadius(r0 + 10)
  .startAngle(groupStartAngle)
  .endAngle(groupEndAngle);

// set up canvas
var svg = d3.select("#chart").append("svg:svg")
  .attr("width", r1 * 2)
  .attr("height", r1 * 2)
  .append("svg:g")
  .attr("transform", "translate(" + r1 + "," + r1 + ")");

// load sample data and initialize
d3.json("jobs.json", function(data) {
  jobs = data;
  initialize();
});

/**
 * Initialize visualization.
 */
function initialize() {
  var jobByName = {},
    indexByName = {},
    nameByIndex = {},
    matrix = [],
    n = 0;

  // Compute a unique index for each job name
  jobs.forEach(function(j) {
    if (!(j.name in indexByName)) {
      nameByIndex[n] = j.name;
      jobByName[j.name] = j;
      indexByName[j.name] = n++;
    }
  });

  // Add predecessor and successor index maps to all jobs
  jobs.forEach(function (j) {
    j.predecessorIndices = {};
    j.successorIndices = {};
  });

  // Construct a square matrix counting dependencies
  for (var i = -1; ++i < n;) {
    var row = matrix[i] = [];
    for (var j = -1; ++j < n;) {
      row[j] = 0;
    }
  }
  jobs.forEach(function(j) {
    var p = indexByName[j.name];
    j.successorNames.forEach(function(n) {
      var s = indexByName[n];
      matrix[s][p]++;

      // initialize predecessor and successor indices
      j.successorIndices[s] = d3.keys(j.successorIndices).length;
      var sj = jobByName[n];
      sj.predecessorIndices[p] = d3.keys(sj.predecessorIndices).length;
    });
  });

  chord.matrix(matrix);

  // override start and end angles for groups and chords
  groups = chord.groups();
  chords = chord.chords();

  for (var i = 0; i < groups.length; i++) {
    var d = groups[i];
    d.startAngle = groupStartAngle(d);
    d.endAngle = groupEndAngle(d);
  }

  function chordAngle(d, f, i, n) {
    var g = groups[d.index];
    var s = g.startAngle;
    var e = g.endAngle;
    var r = (e - s) / 2;
    var ri = r / n;
    return s + r * (f ? 0 : 1) + ri * i;
  }

  for (var i = 0; i < chords.length; i++) {
    var d = chords[i];
    var s = d.source;
    var t = d.target;
    var sj = jobByName[nameByIndex[s.index]];
    var tj = jobByName[nameByIndex[t.index]];
    var si = sj.predecessorIndices[t.index];
    var ti = tj.successorIndices[s.index];
    var sn = d3.keys(sj.predecessorIndices).length;
    var tn = d3.keys(tj.successorIndices).length;
    s.startAngle = chordAngle(s, true, si, sn);
    s.endAngle = chordAngle(s, true, si + 1, sn);
    t.startAngle = chordAngle(t, false, ti, tn);
    t.endAngle = chordAngle(t, false, ti + 1, tn);
  }

  var g = svg.selectAll("g.group")
    .data(groups)
    .enter().append("svg:g")
    .attr("class", "group");

  g.append("svg:path")
    .style("fill", function(d) { return fill(d.index); })
    .style("stroke", function(d) { return fill(d.index); })
    .attr("d", arc);

  g.append("svg:text")
    .each(function(d) { d.angle = (d.startAngle + d.endAngle) / 2; })
    .attr("dy", ".35em")
    .attr("text-anchor", function(d) { return null; })
    .attr("transform", function(d) {
      return "rotate(" + (d.angle * 180 / Math.PI - 90) + ")"
        + "translate(" + (r0 + 26) + ")";
    })
    .text(function(d) { return nameByIndex[d.index]; });

  svg.selectAll("path.chord")
    .data(chords)
    .enter().append("svg:path")
    .attr("class", "chord")
    .style("stroke", function(d) { return d3.rgb(fill(d.source.index)).darker(); })
    .style("fill", function(d) { return fill(d.source.index); })
    .attr("d", d3.svg.chord().radius(r0));

}

d3.select(self.frameElement).style("height", "600px");
