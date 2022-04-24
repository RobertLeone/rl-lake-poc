select
    case
        when date_format(vacina_dataaplicacao, '%W') = 'Monday' then '1. Segunda'
        when date_format(vacina_dataaplicacao, '%W') = 'Tuesday' then '2. Terça'
        when date_format(vacina_dataaplicacao, '%W') = 'Wednesday' then '3. Quarta'
        when date_format(vacina_dataaplicacao, '%W') = 'Thursday' then '4. Quinta'
        when date_format(vacina_dataaplicacao, '%W') = 'Friday' then '5. Sexta'
        when date_format(vacina_dataaplicacao, '%W') = 'Saturday' then '6. Sabádo'
        when date_format(vacina_dataaplicacao, '%W') = 'Sunday' then '7. Domingo'
    end dias,
    count(*) vacinas_aplicadas
from datasus_silver_db.datasus
where {{vacina_dataaplicacao}}
[[AND {{paciente_endereco_uf}}]]
group by 1
order by 1;