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
var jobSelected;
var jobSelectedLastUpdate;
var jobSelectedColor = "#00FF00";

/**
 * Select the given job and update global state.
 */
function selectJob(job) {
  jobSelected = job;
  jobSelectedLastUdpate = new Date().getTime();
  updateJobDialog(job);
}

function isSelected(job) {
  return job === jobSelected;
}

// mouse over job
var jobMouseOver;
var jobMouseOverColor = "#0000BB";

function handleArcMouseOver(d, i) {
  info("Mouse is over index '" + i + "'");
  jobMouseOver = d.job;
  refreshDisplay();
}

function isMouseOver(job) {
  return job === jobMouseOver;
}

function handleArcClick(d, i) {
  selectJob(d.job);
  refreshDisplay();
}

/**
 * Display messages.
 */

function error(msg) { d3.select('#scriptStatusDialog').text(msg); }
function info(msg) { d3.select('#scriptStatusDialog').text(msg); }

/**
 * Updates table with job data.
 */
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
  if (job.index >= 0) {
    $('.job-n-of-n', props).text('job ' + (job.index + 1) + ' of ' + jobs.length);
  } else {
    $('.job-n-of-n', props).text('');
  }
  $('.InnerRight').show();
}

/**
 * Retrieves snapshot of current DAG of scopes from back end.
 */
function loadDag() {
  // load sample data and initialize
  d3.json("small-dag.json", function(data) {
    if (data == null) {
      alert("Failed to load sample data");
      return;
    }
    jobs = data;
    initialize();
    loadTable();
    clearTimeout(loadDagIntervalId);
    startEventPolling();
  });
}

function value(value) {
  if (value == null) {
    return '';
  }
  return value;
}

function loadTable() {
  jobs.forEach(function(job) {
      var rowClass = ''
      if (job.index % 2 != 0) {
          rowClass = 'odd'
      }
      $('#rounded-corner tr:last').after(
         '<tr id="row-num-' + job.index + '" class="' + rowClass + '">'+
          '<td class="row-job-num">' + (job.index+1) + '</td>' +
          '<td class="row-job-id"><a target="_blank" class="job-jt-url"></a></td>' +
          '<td class="row-job-status"/>' +
          '<td class="row-job-alias"/>' +
          '<td class="row-job-feature"/>' +
          '<td class="row-job-mappers"/>' +
          '<td class="row-job-reducers"/>' +
         '</tr>'
      );
      $('#row-num-' + job.index).bind('click', function() {
        selectJob(this.job);
      });

      updateTableRow(job);
  });
}

function updateTableRow(job) {
  var row = $('#row-num-' + job.index);
  $('.job-jt-url', row).text(job.jobId);
  $('.job-jt-url', row).attr('href', job.trackingUrl);
  $('.row-job-status', row).text(value(job.status));
  $('.row-job-alias', row).text(value(job.aliases));
  $('.row-job-feature', row).text(value(job.features));
  $('.row-job-mappers', row).text(buildTaskString(job.totalMappers, job.mapProgress));
  $('.row-job-reducers', row).text(buildTaskString(job.totalReducers, job.reduceProgress));
}

/**
 * Updates job state, registers job with jobId, selects started job, updates
 * display.
 */
function handleJobStartedEvent(event) {
  var job = updateJobData(event.eventData);
  if (job == null) return;
  info(job.jobId + ' started');
  job.jobId = event.eventData.jobId;
  job.status = "RUNNING";
  jobsByJobId[job.jobId] = job;
  selectJob(job);
  refreshDisplay();
}

function handleJobCompleteEvent(event) {
  event.eventData.jobId = event.eventData.jobData.jobId;
  var job = updateJobData(event.eventData);
  if (job == null) return;
  info(job.jobId + ' complete');
  job.status = "COMPLETE";
  var i = job.index + 1;
  if (i < jobs.length) {
    selectJob(jobs[i]);
  }
  refreshDisplay();
}

function handleJobFailedEvent(event) {
  event.eventData.jobId = event.eventData.jobData.jobId;
  var job = updateJobData(event.eventData);
  if (job == null) return;
  info(job.jobId + ' failed');
  job.status = "FAILED";
}

function handleJobProgressEvent(event) {
  var job = updateJobData(event.eventData);
  if (job == null) return;
  if (job.isComplete) {
    if (job.isSuccessful) {
      job.status = "COMPLETE";
    } else {
      job.status = "FAILED";
    }
  }
  if (isSelected(job)) {
    updateJobDialog(job);
  }
}

function handleScriptProgressEvent(event) {
  info('script progress: ' + event.eventData.scriptProgress + '%');
  scriptProgress = event.eventData.scriptProgress;
  $('#progressbar div').width(scriptProgress + '%')
}

/**
 * Looks up job with data.name or data.jobId and updates contents of job with
 * fields from data.
 */
function updateJobData(data) {
  var job, id;
  if (data.name != null) {
    id = data.name;
    job = jobsByName[id];
  } else {
    id = data.jobId;
    job = jobsByJobId[id];
  }
  if (job == null) {
    alert("Job with id '" + id + "' not found");
    return;
  }
  $.each(data, function(key, value) {
    job[key] = value;
  });
  updateTableRow(job);
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
  var scriptDone = false;
  if (scriptProgress == 100) {
    scriptDone = true
    jobs.forEach(function(job) {
        if(job.status != "COMPLETE" && job.status != "FAILED") {
          scriptDone = false;
        }
    });
  }
  if (scriptDone) {
    stopEventPolling();
    return;
  }

  d3.json("event-list.json?lastEventId=" + lastProcessedEventId, function(events) {
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

// group angle initialized once we know the number of jobs
var ga = 0;
var ga2 = 0;
var gap = 0;

// radii of svg figure
var r1 = 400 / 2;
var r0 = r1 - 60;

// define color palette
var fill = d3.scale.category20b();

// job dependencies are visualized by chords
var chord = d3.layout.chord();
var groups;
var chords;

// returns start angle for a chord group
function groupStartAngle(d) {
  return  ga * d.index - ga2;
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
    c = d3.rgb(jobSelectedColor);
  } else if (isMouseOver(d.job)) {
    c = d3.rgb(jobMouseOverColor);
  } else {
    c = d3.interpolateRgb(c, "white")(1/2);
  }
  return c;
}

// more color funcs
function chordStroke(d) { return d3.rgb(jobColor(d.source)).darker(); }
function chordFill(d) { return jobColor(d.source); }

// jobs themselves are arc segments around the edge of the chord diagram
var arcMouse = d3.svg.arc()
  .innerRadius(r0)
  .outerRadius(r0 + 200)
  .startAngle(groupStartAngle)
  .endAngle(groupEndAngle);
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
  .attr("transform", "translate(" + r1 + "," + r1 + ")rotate(90)")
  .append("svg:g")
  .attr("transform", "rotate(0)");

/**
 * Initialize visualization.
 */
function initialize() {
  // initialize group angle
  ga = 2 * Math.PI / jobs.length;
  gap = ga * 0.1;
  ga2 = (ga - gap) / 2;

  // update state
  selectJob(jobs[0]);

  // Compute a unique index for each job name
  var n = 0;
  jobs.forEach(function(j) {
    jobsByName[j.name] = j;
    if (!(j.name in indexByName)) {
      nameByIndex[n] = j.name;
      indexByName[j.name] = j.index = n++;
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

  // add background arc to each g.group to support mouse interaction
  g.append("svg:path")
    .attr("class", "arc-mouse")
    .style("fill", "white")
    .style("stroke", "white")
    .attr("d", arcMouse)
    .on('mouseover', handleArcMouseOver)
    .on('click', handleArcClick);

  // add visual arc to each g.group
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
    .text(function(d) { return d.index + 1; });

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
  /*
  // spin svg to selected job
  var a = (-ga * jobSelected.index) * 180 / Math.PI + 360;
  info("Angle is '" + a + "'");
  svg.transition()
    .duration(1000)
    .attr("transform", "rotate(" + a + ")");
  */
}

function currentRotation() {
  return svg.attr("transform").match(/rotate\(([^\)]+)\)/i)[0];
}

d3.select(self.frameElement).style("height", "500px");

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
  pollIntervalId = setInterval('pollEvents()', 1000);
  return pollIntervalId;
}
