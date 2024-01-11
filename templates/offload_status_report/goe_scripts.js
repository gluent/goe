{% import "offload_status_report/macros.jinja" as macros %}

<script>
var GOENS = {}

GOENS['DATABASE'] = {
    RETAINED_BYTES: {{data['_RETAINED_BYTES']}},
    RECLAIMABLE_BYTES: {{data['_RECLAIMABLE_BYTES']}},
    RETAINED_PARTS: {{data['_RETAINED_PARTS']}},
    RECLAIMABLE_PARTS: {{data['_RECLAIMABLE_PARTS']}},
    RETAINED_ROWS: {{data['_RETAINED_ROWS']}},
    RECLAIMABLE_ROWS: {{data['_RECLAIMABLE_ROWS']}}
};

{% for schema, schema_values in data.items()|sort if not schema.startswith('_') %}
GOENS['{{schema}}'] = {
    RETAINED_BYTES: {{schema_values['_RETAINED_BYTES']}},
    RECLAIMABLE_BYTES: {{schema_values['_RECLAIMABLE_BYTES']}},
    OFFLOADED_BYTES: {{schema_values['_OFFLOADED_BYTES']}},
    RETAINED_PARTS: {{schema_values['_RETAINED_PARTS']}},
    RECLAIMABLE_PARTS: {{schema_values['_RECLAIMABLE_PARTS']}},
    OFFLOADED_PARTS: {{schema_values['_OFFLOADED_PARTS']}},
    RETAINED_ROWS: {{schema_values['_RETAINED_ROWS']}},
    RECLAIMABLE_ROWS: {{schema_values['_RECLAIMABLE_ROWS']}},
    OFFLOADED_ROWS: {{schema_values['_OFFLOADED_ROWS']}}
};
{% endfor %}

$(document).ready(function() {

    $('.btn-toggle').click(function() {
        $(this).find('.btn').toggleClass('active');

        if ($(this).find('.btn-primary').size() > 0) {
            $(this).find('.btn').toggleClass('btn-primary');
        }
        if ($(this).find('.btn-danger').size() > 0) {
            $(this).find('.btn').toggleClass('btn-danger');
        }
        if ($(this).find('.btn-success').size() > 0) {
            $(this).find('.btn').toggleClass('btn-success');
        }
        if ($(this).find('.btn-info').size() > 0) {
            $(this).find('.btn').toggleClass('btn-info');
        }

        $(this).find('.btn').toggleClass('btn-default');
    });

    $(function() {
        $('[data-toggle="tooltip"]').tooltip()
        $('[data-toggle="popover"]').popover()
    });

    $("input[name='optradio'][type=radio]").on('change', function(index) {

        $('.progress-bar[schema_name]').each(function(index) {
            var schema_name = $(this).attr("schema_name");
            var category = $(this).attr("category");
            var option_selected = $("input[name='optradio'][type=radio]:checked").val();

            if (option_selected == 'size') {
                var retained = GOENS[schema_name].RETAINED_BYTES;
                var reclaimable = GOENS[schema_name].RECLAIMABLE_BYTES;
                var offloaded = GOENS[schema_name].OFFLOADED_BYTES;
                var retained_display = (retained / Math.pow(1024, 3)).toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' GB'
                var reclaimable_display = (reclaimable / Math.pow(1024, 3)).toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' GB'
                var rdbms_total_display = (Math.round(retained / Math.pow(1024, 3) * 100) / 100 + Math.round(reclaimable / Math.pow(1024, 3) * 100) / 100).toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' GB'
                var offloaded_display = (offloaded / Math.pow(1024, 3)).toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' GB'
            } else if (option_selected == 'partitions') {
                var retained = GOENS[schema_name].RETAINED_PARTS;
                var reclaimable = GOENS[schema_name].RECLAIMABLE_PARTS;
                var offloaded = GOENS[schema_name].OFFLOADED_PARTS;
                var retained_display = retained.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
                var reclaimable_display = reclaimable.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
                var rdbms_total_display = (retained + reclaimable).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
                var offloaded_display = offloaded.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
            } else if (option_selected == 'rows') {
                var retained = GOENS[schema_name].RETAINED_ROWS;
                var reclaimable = GOENS[schema_name].RECLAIMABLE_ROWS;
                var offloaded = GOENS[schema_name].OFFLOADED_ROWS;
                var retained_display = retained.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
                var reclaimable_display = reclaimable.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
                var rdbms_total_display = (retained + reclaimable).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
                var offloaded_display = offloaded.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
            }

            reclaimable_width = parseFloat(reclaimable) / (parseFloat(reclaimable) + parseFloat(retained)) * 100;
            retained_width = 100 - reclaimable_width;

            if (category == 'reclaimable') {
                $('.progress-bar[schema_name="' + schema_name + '"][category="reclaimable"]').css('width', reclaimable_width + '%')
                $('.reclaimable-display[schema_name="' + schema_name + '"]').text(reclaimable_display)
            } else if (category == 'retained') {
                $('.progress-bar[schema_name="' + schema_name + '"][category="retained"]').css('width', retained_width + '%')
                $('.retained-display[schema_name="' + schema_name + '"]').text(retained_display)
            } else if (category == 'offloaded') {
                $('.offloaded-display[schema_name="' + schema_name + '"]').text(offloaded_display)
            }

            $('.progress-bar[schema_name="' + schema_name + '"]').parent().attr('data-content', rdbms_total_display)

        });
    });

    $('.bucket-schema-button').each(function(index) {
        var target = $(this).attr('data-target')

        $('' + target).on('shown.bs.collapse', function(event) {
            $('' + target + '_button').children().toggleClass('fa-caret-right fa-caret-down');
            event.stopPropagation();
        });

        $('' + target).on('hidden.bs.collapse', function(event) {
            $('' + target + '_button').children().toggleClass('fa-caret-right fa-caret-down');
            event.stopPropagation();
        });
    });

    // c3.js charts
    var chart_partitions = c3.generate({
        bindto: '#reclaim_partitions_chart',
        data: {
            columns: [
                ['Reclaimable', GOENS['DATABASE'].RECLAIMABLE_PARTS],
                ['Retained', GOENS['DATABASE'].RETAINED_PARTS],
                ['Total', parseInt(GOENS['DATABASE'].RECLAIMABLE_PARTS) + parseInt(GOENS['DATABASE'].RETAINED_PARTS)]
            ],
            type: 'bar',
            // Total is not another bar but a line
            types: {
                Total: 'line',
            },
            colors: {
                Reclaimable: '#337ab7',
                Retained: '#f07b4c',
                Total: '#777'
            },
            groups: [
                ['Reclaimable', 'Retained']
            ],
            order: null,
            empty: {
                label: {
                    text: "No Data"
                }
            },
            labels: {
                format: {
                    Reclaimable: function(v, id, i, j) {
                        return '';
                    },
                    Retained: function(v, id, i, j) {
                        return '';
                    },
                    Total: function(v, id, i, j) {
                        return v.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                    }
                }
            }
        },
        axis: {
            x: {
                show: false,
                type: 'category',
                categories: ['Segments']
            },
            y: {
                show: false
            }
        },
        legend: {
            show: false
        },
        title: {
            text: 'Segments'
        },
        tooltip: {
            format: {
                value: function(value, ratio, id) {
                    return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                }
            }
        },
        oninit: function(d, i) {
            $('#reclaim_partitions_loading').toggleClass('hidden');
            $('#reclaim_partitions_chart').toggleClass('invisible');
        },
        onrendered: function(d, i) {
            a0 = d3.select('#reclaim_partitions_chart g.c3-chart-bars g.c3-target-Reclaimable')[0][0].getBBox().height;
            a = d3.select('#reclaim_partitions_chart g.c3-target-Reclaimable text');
            y = (parseFloat(a0) / 2) + parseFloat(a.attr('y'));

            a.html('');
            a.append('tspan').text(GOENS['DATABASE'].RECLAIMABLE_PARTS.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")).attr('y', y).attr('x', '50%');
            a.append('tspan').text('RECLAIMABLE').attr('dy', '1.5em').attr('x', '50%');

            b0 = d3.select('#reclaim_partitions_chart g.c3-chart-bars g.c3-target-Retained')[0][0].getBBox().height;
            b = d3.select('#reclaim_partitions_chart g.c3-target-Retained text');
            y = (parseFloat(b0) / 2) + parseFloat(b.attr('y'));

            b.html('');
            b.append('tspan').text(GOENS['DATABASE'].RETAINED_PARTS.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")).attr('y', y).attr('x', '50%');
            b.append('tspan').text('RETAINED').attr('dy', '1.5em').attr('x', '50%');
        }
    });

    var chart_rows = c3.generate({
        bindto: '#reclaim_rows_chart',
        data: {
            columns: [
                ['Reclaimable', GOENS['DATABASE'].RECLAIMABLE_ROWS],
                ['Retained', GOENS['DATABASE'].RETAINED_ROWS],
                ['Total', parseInt(GOENS['DATABASE'].RECLAIMABLE_ROWS) + parseInt(GOENS['DATABASE'].RETAINED_ROWS)]
            ],
            type: 'bar',
            // Total is not another bar but a line
            types: {
                Total: 'line',
            },
            colors: {
                Reclaimable: '#337ab7',
                Retained: '#f07b4c',
                Total: '#777'
            },
            groups: [
                ['Reclaimable', 'Retained']
            ],
            order: null,
            empty: {
                label: {
                    text: "No Data"
                }
            },
            labels: {
                format: {
                    Reclaimable: function(v, id, i, j) {
                        return '';
                    },
                    Retained: function(v, id, i, j) {
                        return '';
                    },
                    Total: function(v, id, i, j) {
                        return v.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                    }
                }
            }
        },
        axis: {
            x: {
                show: false,
                type: 'category',
                categories: ['Rows']
            },
            y: {
                show: false
            }
        },
        legend: {
            show: false
        },
        title: {
            text: 'Rows'
        },
        tooltip: {
            format: {
                value: function(value, ratio, id) {
                    return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                }
            }
        },
        oninit: function(d, i) {
            $('#reclaim_rows_loading').toggleClass('hidden');
            $('#reclaim_rows_chart').toggleClass('invisible');
        },
        onrendered: function(d, i) {
            c0 = d3.select('#reclaim_rows_chart g.c3-chart-bars g.c3-target-Reclaimable')[0][0].getBBox().height;
            c = d3.select('#reclaim_rows_chart g.c3-target-Reclaimable text');
            y = (parseFloat(c0) / 2) + parseFloat(c.attr('y'));

            c.html('');
            c.append('tspan').text(GOENS['DATABASE'].RECLAIMABLE_ROWS.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")).attr('y', y).attr('x', '50%');
            c.append('tspan').text('RECLAIMABLE').attr('dy', '1.5em').attr('x', '50%');

            d0 = d3.select('#reclaim_rows_chart g.c3-chart-bars g.c3-target-Retained')[0][0].getBBox().height;
            d = d3.select('#reclaim_rows_chart g.c3-target-Retained text');
            y = (parseFloat(d0) / 2) + parseFloat(d.attr('y'));

            d.html('');
            d.append('tspan').text(GOENS['DATABASE'].RETAINED_ROWS.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")).attr('y', y).attr('x', '50%');
            d.append('tspan').text('RETAINED').attr('dy', '1.5em').attr('x', '50%');
        }
    });

    var chart_size = c3.generate({
        bindto: '#reclaim_size_chart',
        data: {
            columns: [
                ['Reclaimable', GOENS['DATABASE'].RECLAIMABLE_BYTES],
                ['Retained', GOENS['DATABASE'].RETAINED_BYTES],
                ['Total', parseFloat(GOENS['DATABASE'].RECLAIMABLE_BYTES) + parseFloat(GOENS['DATABASE'].RETAINED_BYTES)]
            ],
            type: 'bar',
            // Total is not another bar but a line
            types: {
                Total: 'line',
            },
            colors: {
                Reclaimable: '#337ab7',
                Retained: '#f07b4c',
                Total: '#777'
            },
            groups: [
                ['Reclaimable', 'Retained']
            ],
            order: null,
            empty: {
                label: {
                    text: "No Data"
                }
            },
            labels: {
                format: {
                    Reclaimable: function(v, id, i, j) {
                        return '';
                    },
                    Retained: function(v, id, i, j) {
                        return '';
                    },
                    Total: function(v, id, i, j) {
                        return parseFloat(v / 1024 / 1024 / 1024).toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' GB';
                    }
                }
            }
        },
        axis: {
            x: {
                show: false,
                type: 'category',
                categories: ['Size']
            },
            y: {
                show: false
            }
        },
        legend: {
            show: false
        },
        title: {
            text: 'Size'
        },
        tooltip: {
            format: {
                value: function(value, ratio, id) {
                    return (value / 1024 / 1024 / 1024).toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' GB';
                }
            }
        },
        oninit: function(d, i) {
            $('#reclaim_size_loading').toggleClass('hidden');
            $('#reclaim_size_chart').toggleClass('invisible');
        },
        onrendered: function(d, i) {
            c0 = d3.select('#reclaim_size_chart g.c3-chart-bars g.c3-target-Reclaimable')[0][0].getBBox().height;
            c = d3.select('#reclaim_size_chart g.c3-target-Reclaimable text');
            y = (parseFloat(c0) / 2) + parseFloat(c.attr('y'));

            c.html('');
            c.append('tspan').text(parseFloat(GOENS['DATABASE'].RECLAIMABLE_BYTES / 1024 / 1024 / 1024).toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' GB').attr('y', y).attr('x', '50%');
            c.append('tspan').text('RECLAIMABLE').attr('dy', '1.5em').attr('x', '50%');

            d0 = d3.select('#reclaim_size_chart g.c3-chart-bars g.c3-target-Retained')[0][0].getBBox().height;
            d = d3.select('#reclaim_size_chart g.c3-target-Retained text');
            y = (parseFloat(d0) / 2) + parseFloat(d.attr('y'));

            d.html('');
            d.append('tspan').text(parseFloat(GOENS['DATABASE'].RETAINED_BYTES / 1024 / 1024 / 1024).toFixed(2).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") + ' GB').attr('y', y).attr('x', '50%');
            d.append('tspan').text('RETAINED').attr('dy', '1.5em').attr('x', '50%');
        }
    });

    var gauge = c3.generate({
        bindto: '#osr_gauge',
        data: {
            columns: [
                ['data', {{macros.pct((data['_RETAINED_BYTES'] + data['_RECLAIMABLE_BYTES']), data['_OFFLOADED_BYTES'], 'no')}}],
                ['data2', {{macros.pct(data['_RETAINED_BYTES'], (data['_RECLAIMABLE_BYTES'] + data['_OFFLOADED_BYTES']), 'no')}}]
            ],
            type: 'gauge',
            colors: {
                'data': '#337ab7',
                'data2': '#f07b4c'
            }
        },
        gauge: {
            label: {
                show: false
            },
            width: 85
        },
        size: {
            height: 176
        },
        tooltip: {
            show: false
        },
        interaction: {
            enabled: false
        },
        oninit: function(d, i) {
            $('#osr_gauge_loading').toggleClass('hidden');
            $('#osr_gauge_container').toggleClass('invisible');
        }
    });

    d3.select('text.c3-gauge-value').remove();
    d3.select('text.c3-gauge-value').remove();

    // List.js inits
    GOENS['schemas'] = {}
    GOENS['schemas']['options'] = {
        valueNames: ['table_name',
            {
                attr: 'data-offload_type',
                name: 'offload_type'
            },
            {
                attr: 'data-offload_partitions',
                name: 'offload_partitions'
            },
            {
                attr: 'data-offload_gbytes',
                name: 'offload_gbytes'
            },
            {
                attr: 'data-retain_partitions',
                name: 'retain_partitions'
            },
            {
                attr: 'data-retain_gbytes',
                name: 'retain_gbytes'
            },
            {
                attr: 'data-reclaim_partitions',
                name: 'reclaim_partitions'
            },
            {
                attr: 'data-reclaim_gbytes',
                name: 'reclaim_gbytes'
            }
        ]
    };
    GOENS['schemas']['sort_key'] = 'table_name';
    GOENS['schemas']['sort_direction'] = 'asc';

    $('.bucket-schema').each(function(index) {
        var target = $(this).attr('id');
        var section = $(this).attr('osr-section');
        GOENS[target] = new List(target, GOENS[section]['options']);
        GOENS[target].sort(GOENS[section]['sort_key'], {
            order: GOENS[section]['sort_direction']
        });
    });
});

</script>
