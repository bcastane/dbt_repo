{% macro add_date_if_underscore(alias) %}
 
        {# Import the datetime module #}
        {% set timezone = modules.pytz.timezone('America/Santiago') %}

        {% set now_santiago = modules.datetime.datetime.now(timezone) %}

        {# Format the date as YYYY-MM-DD #}
        {% set current_date = now_santiago.strftime('%Y%m%d') %}

        {# Check if the alias ends with an underscore and append the date if true #}
        {% if alias.endswith('_') %}
            {{ alias + current_date }}
        {% else %}
            {{ alias }}
        {% endif %}
  
{% endmacro %}
