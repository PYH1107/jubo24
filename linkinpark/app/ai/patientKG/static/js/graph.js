function default_function() {
  var disease = "Diabetes";
  console.log(disease);

  $.ajax({
    type: "GET",
    url: "/visual",
    data: { input_disease: disease },
    success: function (result) {
      var style = [
        { selector: 'node[label = "patient"]', css: { 'background-color': '#54b4bd', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "strong_entity"]', css: { 'background-color': '#616161', 'background-opacity': '1', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "medium_entity"]', css: { 'background-color': '#616161', 'background-opacity': '0.6', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "weak_entity"]', css: { 'background-color': '#616161', 'background-opacity': '0.2', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
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
  var disease = document.getElementById('selected_disease').value;
  console.log(disease);

  $.ajax({
    type: "GET",
    url: "/visual",
    data: { input_disease: disease },
    success: function (result) {
      var style = [
        { selector: 'node[label = "patient"]', css: { 'background-color': '#54b4bd', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "strong_entity"]', css: { 'background-color': '#616161', 'background-opacity': '1', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "medium_entity"]', css: { 'background-color': '#616161', 'background-opacity': '0.6', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
        { selector: 'node[label = "weak_entity"]', css: { 'background-color': '#616161', 'background-opacity': '0.2', 'content': 'data(name)', 'text-halign': 'center', 'text-valign': 'center' } },
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