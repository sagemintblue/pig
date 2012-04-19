// globals
var backendBaseUrl = "http://localhost:8080";
var lastProcessedEventId = -1;

// storage for job data and lookup
var jobs;
var jobsByName = {};
var jobsByJobId = {};
var scriptProgress = 0;
var indexByName = {};
var nameByIndex = {};
var matrix = [];

// currently selected job
var selectedJob;
var selectedJobLastUpdate;
var selectedColor = "#00FF00";

function updateJobDialog(job) {
  var props = $('.job-prop-list');
  $('.job-jt-url', props).text(job.jobId);
  $('.job-jt-url', props).attr('href', job.trackingUrl);
  $('.job-alias', props).text(job.aliases);
  $('.job-feature', props).text(job.features);
  $('.job-scope', props).text(job.name);
  $('.job-mapper-status', props).text(buildTaskString(job.totalMappers, job.mapProgress));
  $('.job-reducer-status', props).text(buildTaskString(job.totalReducers, job.reduceProgress));
  $('.job-status', props).text(job.status);
  $('.InnerRight').show();
}

/**
 * Select the given job and update global state.
 */
function selectJob(job) {
  selectedJob = job;
  selectedJobLastUdpate = new Date().getTime();
  updateJobDialog(job);
}

function isSelected(j) {
  return j === selectedJob;
}

/**
 * Displays an error message.
 */
function displayError(msg) {
  // TODO(Andy Schlaikjer): Display error, pause event polling
  alert(msg);
}

/**
 * Retrieves snapshot of current DAG of scopes from back end.
 */
function loadDag() {
  // load sample data and initialize
  d3.json("pig-dag.json", function(data) {
    if (data == null) {
      alert("Failed to load sample data");
      return;
    }
    jobs = data;
    initialize();
    clearTimeout(loadDagIntervalId);
    startEventPolling();
  });
}


// TODO(Andy Schlaikjer): update dag state based on event and trigger viz updates
function handleJobStartedEvent(event) {
  d3.select('#updateDialog').text(event.eventData.jobId + ' started');
  var name = event.eventData.name;
  var job = jobsByName[name];
  if (job == null) {
    alert("Job with name '" + name + "' not found");
    return;
  }
  job.jobId = event.eventData.jobId;
  job.status = "RUNNING";
  jobsByJobId[job.jobId] = job;
  updateJobData(event.eventData);
  selectJob(job);
  refreshDisplay();
}

function handleJobCompleteEvent(event) {
  event.eventData.jobId = event.eventData.jobData.jobId;
  var job = updateJobData(event.eventData);
  job.status = "COMPLETE";
  d3.select('#updateDialog').text(job.jobId + ' complete');

  // TODO
  job = jobsByName[job.name];
  var i = job.index + 1;
  if (i >= jobs.length) {
    // jump to first job if we've reached the end
    i = 0;
  }
  selectJob(jobs[i]);
  refreshDisplay();
}

function handleJobFailedEvent(event) {
  event.eventData.jobId = event.eventData.jobData.jobId;
  var job = updateJobData(event.eventData);
  job.status = "FAILED";
  d3.select('#updateDialog').text(job.jobId + ' failed');
}

function handleJobProgressEvent(event) {
  var job = updateJobData(event.eventData);
  d3.select('#updateDialog')
    .text(job.jobId + ' map progress: ' + job.mapProgress * 100 + '%'
      + ' reduce progress: ' + job.reduceProgress * 100 + '%');
}

function handleScriptProgressEvent(event) {
  d3.select('#scriptStatusDialog')
      .text('script progress: ' + event.eventData.scriptProgress + '%');
  scriptProgress = event.eventData.scriptProgress;
}

// looks up the job from data.jobId and updates all data fields onto job
function updateJobData(data) {
  var job = jobsByJobId[data.jobId];
  if (job == null) {
    alert("Job with name '" + name + "' not found");
    return;
  }
  $.each(data, function(key, value) {
    job[key] = value;
  });
  return job
}

function hideJobDialog() {
  $('.InnerRight').hide();
}

function buildTaskString(total, progress) {
  if (total == null || progress == null) {
    return ''
  }
  return total + ' (' + progress*100 + '%)'
}

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
        } else if(eventType == "JOB_FINISHED") {
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

// group angle initialized once we know the number of jobs
var ga = 0;
var ga2 = 0;
var gap = 0;

// radii of svg figure
var r1 = 600 / 2;
var r0 = r1 - 120;

// define color palette
var fill = d3.scale.category20b();

// job dependencies are visualized by chords
var chord = d3.layout.chord();
var groups;
var chords;

// returns start angle for a chord group
function groupStartAngle(d) {
  return  ga * d.index + gap + Math.PI / 2 - ga2;
}

// returns end angle for a chord group
function groupEndAngle(d) {
  return groupStartAngle(d) + ga - gap;
}

/**
 * @param d chord data
 * @param f boolean flag indicating chord is out-link
 * @param i chord in- / out-link index within current group
 * @param n in- / out-degree of current group
 */
function chordAngle(d, f, i, n) {
  var g = groups[d.index];
  var s = g.startAngle;
  var e = g.endAngle;
  var r = (e - s) / 2;
  var ri = r / n;
  return s + r * (f ? 0 : 1) + ri * i;
}

// returns color for job arc and chord
function jobColor(d) {
  var c = fill(d.index);
  if (isSelected(d.job)) {
    c = d3.rgb(selectedColor);
  } else {
    c = d3.interpolateRgb(c, "white")(1/2);
  }
  return c;
}

// more color funcs
function chordStroke(d) { return d3.rgb(jobColor(d.source)).darker(); }
function chordFill(d) { return jobColor(d.source); }

// jobs themselves are arc segments around the edge of the chord diagram
var arc = d3.svg.arc()
  .innerRadius(r0)
  .outerRadius(r0 + 10)
  .startAngle(groupStartAngle)
  .endAngle(groupEndAngle);

// set up canvas
var svg = d3.select("#chart")
  .append("svg:svg")
  .attr("width", r1 * 2)
  .attr("height", r1 * 2)
  .append("svg:g")
  .attr("transform", "translate(" + r1 + "," + r1 + ")");

/**
 * Initialize visualization.
 */
function initialize() {
  // initialize group angle
  ga = 2 * Math.PI / jobs.length;
  ga2 = ga / 2;
  gap = ga2 * 0.2;

  // update state
  selectJob(jobs[0]);

  // Compute a unique index for each job name
  var n = 0;
  jobs.forEach(function(j) {
    jobsByName[j.name] = j;
    if (!(j.name in indexByName)) {
      nameByIndex[n] = j.name;
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
      var sj = jobsByName[n];
      sj.predecessorIndices[p] = d3.keys(sj.predecessorIndices).length;
    });
  });

  chord.matrix(matrix);

  // override start and end angles for groups and chords
  groups = chord.groups();
  chords = chord.chords();

  // initialize groups
  for (var i = 0; i < groups.length; i++) {
    var d = groups[i];
    
    // associate group with job
    d.job = jobs[i];

    // angles
    d.startAngle = groupStartAngle(d);
    d.endAngle = groupEndAngle(d);
  }

  // initialize begin / end angles for chord source / target
  for (var i = 0; i < chords.length; i++) {
    var d = chords[i];
    var s = d.source;
    var t = d.target;

    // associate jobs with chord source and target objects
    var sj = jobsByName[nameByIndex[s.index]];
    var tj = jobsByName[nameByIndex[t.index]];
    s.job = sj;
    t.job = tj;

    // determine chord source and target indices
    var si = sj.predecessorIndices[t.index];
    var ti = tj.successorIndices[s.index];

    // determine chord source out-degree and target in-degree
    var sn = d3.keys(sj.predecessorIndices).length;
    var tn = d3.keys(tj.successorIndices).length;
    s.startAngle = chordAngle(s, true, si, sn);
    s.endAngle = chordAngle(s, true, si + 1, sn);
    t.startAngle = chordAngle(t, false, ti, tn);
    t.endAngle = chordAngle(t, false, ti + 1, tn);
  }

  // select an svg g element for each group
  var g = svg.selectAll("g.group")
    .data(groups)
    .enter()
    .append("svg:g")
    .attr("class", "group");

  // add an arc to each g.group
  g.append("svg:path")
    .attr("class", "arc")
    .style("fill", jobColor)
    .style("stroke", jobColor)
    .attr("d", arc);

  // add a label to each g.group
  g.append("svg:text")
    .each(function(d) { d.angle = (d.startAngle + d.endAngle) / 2; })
    .attr("dy", ".35em")
    .attr("text-anchor", null)
    .attr("transform", function(d) {
      return "rotate(" + (d.angle * 180 / Math.PI - 90) + ")"
        + "translate(" + (r0 + 26) + ")";
    })
    .text(function(d) { return nameByIndex[d.index]; });

  // add chords
  svg.selectAll("path.chord")
    .data(chords)
    .enter()
    .append("svg:path")
    .attr("class", "chord")
    .style("stroke", chordStroke)
    .style("fill", chordFill)
    .attr("d", d3.svg.chord().radius(r0));
}

/**
 * Refreshes the visual elements based on current (updated) state.
 */
function refreshDisplay() {
  // update path.arc elements
  svg.selectAll("path.arc")
    .transition()
    .style("fill", jobColor)
    .style("stroke", jobColor);

  // update path.chord elements
  svg.selectAll("path.chord")
    .transition()
    .style("stroke", chordStroke)
    .style("fill", chordFill);
}

d3.select(self.frameElement).style("height", "600px");

var loadDagIntervalId;
var pollIntervalId;

$(document).ready(function() {
  hideJobDialog();
  loadDagTimeoutId = setTimeout('loadDag()', 500);
});

function stopEventPolling() {
  clearInterval(pollIntervalId);
  return pollIntervalId;
}

function startEventPolling() {
  pollIntervalId = setInterval('pollEvents()', 500);
  return pollIntervalId;
}
