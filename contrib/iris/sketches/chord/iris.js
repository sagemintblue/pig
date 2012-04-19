// globals
var backendBaseUrl = "http://localhost:8080";
var lastEventId = 0;

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
      displayError("Failed to retrieve scope graph");
      return;
    }

    // TODO(Andy Schlaikjer): Replace existing scope graph data with new data and update viz
  });
}

/** 
 * Polls back end for new events.
 */
function pollEvents() {
  d3.json(backendBaseUrl + "/events", function(d) {
    // test for error
    if (d == null) {
      displayError("Failed to poll events");
      return;
    }

    // TODO(Andy Schlaikjer): update dag state based on event and trigger viz updates
  });
}

// kick off event poller and keep track of interval id so we can pause polling if needed
//var pollEventsIntervalId = setInterval(pollEvents, 10000);

// storage for job data
var jobs;

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

// returns start angle for a chord group
function groupStartAngle(d) {
  return  ga * d.index + gap + Math.PI / 2 - ga2;
}

// returns end angle for a chord group
function groupEndAngle(d) {
  return groupStartAngle(d) + ga - gap;
}

// jobs themselves are arc segments around the edge of the chord diagram
var arc = d3.svg.arc()
  .innerRadius(r0)
  .outerRadius(r0 + 10)
  .startAngle(groupStartAngle)
  .endAngle(groupEndAngle);

// set up canvas
var svg = d3.select("body")
  .append("svg:svg")
  .attr("width", r1 * 2)
  .attr("height", r1 * 2)
  .append("svg:g")
  .attr("class", "iris_transform")
  .attr("transform", "translate(" + r1 + "," + r1 + ")");

// load sample data and initialize
d3.json("jobs.json", function(data) {
  if (data == null) {
    displayError("Failed to load jobs data");
    return;
  }
  jobs = data;
  initialize();
});

/**
 * Initialize visualization.
 */
function initialize() {
  // initialize group angle
  ga = 2 * Math.PI / jobs.length;
  ga2 = ga / 2;
  gap = ga2 * 0.2;

  // storage for various maps
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
