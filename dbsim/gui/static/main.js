
var SUCCESS = 0;
var FAILURE = -1;

String.prototype.format = function (args) {
  var newStr = this;
  for (var key in args) {
      newStr = newStr.replace('{' + key + '}', args[key]);
  }
  return newStr;
}

function showResults(json) {
  var header_template = `
    <th scope="col">{header}</th>
  `
  var content_col_template = `
    <td>{value}</td>
  `
  var row_number_col_template = `
    <th scope="row">{row_number}</th>
  `
  var table_row_template = `
    <tr>
      {row}
    </tr>
  `
  var outer_template = `
    <thead>
      <tr>
        {headers}
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  `
  
  var headers_html = ``
  for (let i = 0; i <= json['headers'].length; i++) {
    if (i == 0) {
      headers_html += header_template.format({header: "#\n"});
    }
    else {
      headers_html += header_template.format({header: json['headers'][i-1]+"\n"});
    }
  }

  var table_rows_html = ``;
  for (let i = 0; i < json['results'].length; i++) {
    var row_html = row_number_col_template.format({row_number: i+1})+"\n";
    for (let j = 0; j < json['results'][i].length; j++) {
      row_html += content_col_template.format({value: json['results'][i][j]})+"\n";
    }
    var table_row_html = table_row_template.format({row: row_html});
    table_rows_html += table_row_html + "\n";
  }

  var result_table_html = outer_template.format({
    headers: headers_html, 
    rows: table_rows_html
  });

  $( "#query-output-table" ).html(result_table_html);

}

function appendHistoryRecord(json) {
  var history_record_template = `
  <tr>
    <th scope="row">{record_id}</th>
    <td>{query}</td>
    <td>{rules}</td>
    <td>{plan_cost}</td>
    <td>{best_cost}</td>
    <td>{exec_time}</td>
  </tr>
  `
  
  var cur_rules = [];
  $("#rules-list li").each(function() { cur_rules.push($(this).text()) });
  var cur_query = $('#query-input-textarea').val();
  var plan_cost = json['plan_cost'];
  var best_cost = json['best_cost'];
  var exec_time = json['exec_time'];

  var last_record = $("#history-records").find("tbody").find("tr:last-child");
  var last_record_id = 0;
  if (last_record.length > 0) {
    last_record_id = parseInt(last_record.find("th").text());
  }

  var new_history_record = history_record_template.format({
    record_id: (last_record_id+1).toString(),
    query: cur_query,
    rules: cur_rules.join("|"), 
    plan_cost: plan_cost,
    best_cost: best_cost,
    exec_time: exec_time
  });

  $("#history-records").find("tbody").append(new_history_record);

}

function showRules(json) {
  for (let i = 0; i < json['applied_rules'].length; i++) {
    addAppliedRule(json['applied_rules'][i]);
  }
  for (let i = 0; i < json['non_applied_rules'].length; i++) {
    addNonAppliedRule(json['non_applied_rules'][i]);
  }
}

function getAppliedRules() {
  // get the rule name list shown in UI 
  var rule_names_list = [];
  $("#rules-list li.rule").each(function(){
    rule_names_list.push($(this).text());
  });
  return rule_names_list;
}

function requestRules() {
  // request the current applied and non-applied (i.e., all rules excluding the applied ones) rules from the backend query planner
  $.ajax({
    method: "GET",
    url: "/rules",
    dataType: "json",
    contentType: "application/json"              
  }).done(
    function (data) { 
      showRules(data);
    }
  ).fail(
    function (data) { alert(data["msg"]); }
  );
}

function updateRules(new_applied_rules_list) {
  // update the current rules in the backend query planner
  $.ajax({
    method: "POST",
    url: "/rules",
    dataType: "json",
    data: JSON.stringify({
      applied_rules: new_applied_rules_list 
    }),
    contentType: "application/json"              
  }).done(
    function (data) { 
    }
  ).fail(
    function (data) { alert(data["msg"]); }
  );
}

function moveUp($item) {
  $before = $item.prev();
  $item.insertBefore($before);
}

function moveDown($item) {
  $after = $item.next();
  $item.insertAfter($after);
}

function bindActionsToRuleList() {
  $("li.rule").css('cursor', 'pointer');
  $("li.rule")
  .click(function() {
    $(this).siblings("li.active").removeClass("active");
    $(this).addClass("active");
  });
}

function addAppliedRule(rule_name) {
  rule_html = `<li class="list-group-item rule">${rule_name}</li>`
  $("#rules-list").append(rule_html);
  bindActionsToRuleList();
}
function addNonAppliedRule(rule_name) {
  rule_html = `<li class="list-group-item rule">${rule_name}</li>`
  $("#candidate-rules-list").append(rule_html);
  bindActionsToRuleList();
}
function removeRule($selected_rule) {
  $selected_rule.remove();
}

function executeQuery() {
  $.ajax({
    method: "POST",
    url: "/query",
    dataType: "json",
    data: JSON.stringify({ query: $('#query-input-textarea').val() }),
    contentType: "application/json"              
  }).done(
    function (data) { 
      showResults(data);
      $("#plan-viz").attr("src", "static/img/plan.gv.svg?timestamp=" + new Date().getTime());
      $("#best-plan-viz").attr("src", "static/img/best_plan.gv.svg?timestamp=" + new Date().getTime());
      appendHistoryRecord(data);
    }
  ).fail(
    function (data) { alert("Query execution failed, please check if your query has syntax errors or database tables do not exist."); }
  );
}

function requestDatasets() {
  $.ajax({
       url : '/ds',
       type : 'GET',
       dataType: "json"
    }).done(function(data) {
          if (parseInt(data["status"]) == SUCCESS) {
            for (let i = 0; i < data["datasets"].length; i++) {
              addDataset(data["datasets"][i]);
            }
          }
          else {
            alert(data['msg']);
          }
       });
}

function uploadDataset($ds_input) {
  var ds_name_parts = $ds_input.val().split("/");
  if (ds_name_parts.length == 1) {
    ds_name_parts = $ds_input.val().split("\\");
  }
  var ds_name_with_ext = ds_name_parts[ds_name_parts.length-1].split(".");
  var ds_name = "";
  if (ds_name_with_ext.length > 1) {
    ds_name = ds_name_with_ext[0];
  }
  var formData = new FormData();
  formData.append('file', $ds_input[0].files[0]);

  $.ajax({
       url : '/ds',
       type : 'POST',
       data : formData,
       dataType: "json",
       processData: false,  // tell jQuery not to process the data
       contentType: false  // tell jQuery not to set contentType
    }).done(function(data) {
          if (parseInt(data["status"]) == SUCCESS) {
            addDataset(ds_name);
          }
          else {
            alert(data['msg']);
          }
       });
}

function removeDataset($selected_ds) {
  var selected_ds_name = $selected_ds.text();
  $.ajax({
       url : '/ds?name=' + encodeURIComponent(selected_ds_name),
       type : 'DELETE',
       dataType: "json"
    }).done(function(data) {
          if (parseInt(data["status"]) == SUCCESS) {
            $selected_ds.remove();
          }
          else {
            alert(data['msg']);
          }
       });
}

function addDataset(ds_name) {
  ds_html = `<li class="list-group-item rule">${ds_name}</li>`
  $("#datasets-list").append(ds_html);
  bindActionsToRuleList();
}

$(document).ready(function(){

  example_query = `
  SELECT musical.title, musical.year
  FROM 
    (SELECT * FROM 
      (SELECT * FROM animation, musical 
      WHERE animation.mid = musical.mid) 
    WHERE animation.embedding to [1,2,3,4] < 10)
  WHERE musical.year > 1960
  `
  $('#query-input-textarea').val(example_query);
        
  $( "#query-input-textarea" )
  .focusout(function() {
    executeQuery();
  });

  requestRules();

  $( "#rule-up-button" )
  .click(function() {
    var selected_rule = $('#rules-list').find('li.active');
    if (selected_rule.length > 0) {
      moveUp(selected_rule);
      var cur_applied_rules_list = getAppliedRules();
      updateRules(cur_applied_rules_list);
      executeQuery();
    }
  });

  $( "#rule-down-button" )
  .click(function() {
    var selected_rule = $('#rules-list').find('li.active');
    if (selected_rule.length > 0) {
      moveDown(selected_rule);
      var cur_applied_rules_list = getAppliedRules();
      updateRules(cur_applied_rules_list);
      executeQuery();
    }
  });

  $("#rule-adding-button").click(function () {
    $('#rule-adding-modal').modal('show');
  });

  $("div.modal").find("button.close").click(function (){
    $(this).parents("div.modal").modal('hide');
  });

  $("#rule-adding-modal").find("button.add").click(function (){
    var selected_rule = $("#rule-adding-modal").find("div.modal-body").find("li.active");
    if (selected_rule.length == 0) {
      alert("Please select the rule to add.");
      return false;
    }
    var selected_rule_name = selected_rule.text();
    addAppliedRule(selected_rule_name);
    removeRule(selected_rule);
    $("#rule-adding-modal").modal('hide');
    var cur_applied_rules_list = getAppliedRules();
    updateRules(cur_applied_rules_list);
    executeQuery();
  });

  $('#rule-adding-modal').on('hidden.bs.modal', function() {
    var selected_rule = $("#rule-adding-modal").find("div.modal-body").find("li.active");
    if (selected_rule.length > 0) {
      selected_rule.removeClass('active');
    }
  });

  $("#rule-removing-button").click(function () {
    var selected_rule = $("#rules-list").find("li.active");
    if (selected_rule.length == 0) {
      alert("No rule selected");
      return false;
    }
    var selected_rule_name = selected_rule.text();
    removeRule(selected_rule);
    addNonAppliedRule(selected_rule_name);
    var cur_applied_rules_list = getAppliedRules();
    updateRules(cur_applied_rules_list);
    executeQuery();
  });

  requestDatasets()

  $("#manage-datasets-button").click(function () {
    $('#manage-datasets-modal').modal('show');
  });

  $("#add-dataset-fileinput").on('change', function() {
    uploadDataset($(this));
  });

  $("#manage-datasets-modal").find("button.add").click(function (){
    $("#add-dataset-fileinput").trigger("click");
  });

  $("#manage-datasets-modal").find("button.remove").click(function (){
    var selected_ds = $("#manage-datasets-modal").find("div.modal-body").find("li.active");
    if (selected_ds.length == 0) {
      alert("Please select the dataset to remove.");
      return false;
    }
    removeDataset(selected_ds);
  });

  $('#manage-datasets-modal').on('hidden.bs.modal', function() {
    var selected_ds = $("#manage-datasets-modal").find("div.modal-body").find("li.active");
    if (selected_ds.length > 0) {
      selected_ds.removeClass('active');
    }
  });

});
