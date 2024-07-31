function default_function() {
  var focus = "體溫過高";
  console.log(focus);

  $.ajax({
    type: "GET",
    url: "/visual",
    data: { input_focus: focus },
    success: function (result) {
      var style = [
        { selector: 'node[label = "focus"]', css: { 'background-color': '#f17c8c', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "module"]', css: { 'background-color': '#54b4bd', 'background-opacity': '1', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "action"]', css: { 'background-color': '#616161', 'background-opacity': '1', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "item"]', css: { 'background-color': '#dbad61', 'background-opacity': '1', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'edge', css: { 'content': 'data(relationship)', 'target-arrow-shape': 'triangle' } },
      ];

      var cy = cytoscape({
        container: document.getElementById('cy'),
        style: style,
        layout: { name: 'cose' },
        elements: result.elements,
      });
    }
  });
}

function change_function() {
  var focus = document.getElementById('selected_focus').value;
  console.log(focus);

  $.ajax({
    type: "GET",
    url: "/visual",
    data: { input_focus: focus },
    success: function (result) {
      var style = [
        { selector: 'node[label = "focus"]', css: { 'background-color': '#f17c8c', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "module"]', css: { 'background-color': '#54b4bd', 'background-opacity': '1', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "action"]', css: { 'background-color': '#616161', 'background-opacity': '1', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "item"]', css: { 'background-color': '#dbad61', 'background-opacity': '1', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'edge', css: { 'content': 'data(relationship)', 'target-arrow-shape': 'triangle' } },
      ];

      var cy = cytoscape({
        container: document.getElementById('cy'),
        style: style,
        layout: { name: 'cose' },
        elements: result.elements,
      });
    }
  });
}