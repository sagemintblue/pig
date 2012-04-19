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

/** 
 * Polls back end for new events.
 */
function pollEvents() {
  d3.json(backendBaseUrl + "/events", function(d) {
    // test for error
    if (d == null) {
      displayError("Failed to poll events")
      return
    }

    // TODO(Andy Schlaikjer): update dag state based on event and trigger viz updates
  });
}

// kick off event poller and keep track of interval id so we can pause polling if needed
//var pollEventsIntervalId = setInterval(pollEvents, 10000);

// storage for job data
var jobs;

// set up viz params
var r1 = 600 / 2;
var r0 = r1 - 120;

function groupStartAngle(d) {
  return (2 * Math.PI / jobs.length) * d.index;
}

function groupEndAngle(d) {
  var r = 2 * Math.PI / jobs.length;
  return r * (d.index + 1) - r * 0.1;
}

// define color palette
var fill = d3.scale.category20c();

// job dependencies are visualized by chords
var chord = d3.layout.chord()
  .padding(0.04)
  .sortSubgroups(d3.descending)
  .sortChords(d3.descending);

// jobs themselves are arc segments around the edge of the chord diagram
var arc = d3.svg.arc()
  .innerRadius(r0)
  .outerRadius(r0 + 10)
  .startAngle(groupStartAngle)
  .endAngle(groupEndAngle);

// set up canvas
var svg = d3.select("body").append("svg:svg")
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
  var indexByName = {},
    nameByIndex = {},
    matrix = [],
    n = 0;

  // Returns the job index from a full job identifier
  function name(name) {
    return name;
  }

  // Compute a unique index for each job index
  jobs.forEach(function(j) {
    var id = name(j.name);
    if (!(id in indexByName)) {
      nameByIndex[n] = id;
      indexByName[id] = n++;
    }
  });

  // Construct a square matrix counting dependencies
  for (var i = -1; ++i < n;) {
    var row = matrix[i] = [];
    for (var j = -1; ++j < n;) {
      row[j] = 0;
    }
  }
  jobs.forEach(function(j) {
    var pid = indexByName[name(j.name)];
    if (j.successorNames.length == 0) {
//      j.successorNames[0] = j.name
    }
    j.successorNames.forEach(function(n) {
      matrix[indexByName[name(n)]][pid]++;
    });
  });

  chord.matrix(matrix);

  // override start and end angles for groups and chords
  groups = chord.groups();

  for (var i = 0; i < groups.length; i++) {
    var d = groups[i];
    d.startAngle = groupStartAngle(d);
    d.endAngle = groupEndAngle(d);
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
    .data(chord.chords)
    .enter().append("svg:path")
    .attr("class", "chord")
    .style("stroke", function(d) { return d3.rgb(fill(d.source.index)).darker(); })
    .style("fill", function(d) { return fill(d.source.index); })
    .attr("d", d3.svg.chord().startAngle(groupStartAngle).endAngle(groupEndAngle).radius(r0));

}

d3.select(self.frameElement).style("height", "600px");
