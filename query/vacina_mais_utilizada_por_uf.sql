select 
    paciente_endereco_uf,
    count(*)
from datasus_silver_db.datasus
where {{vacina_dataaplicacao}}
[[AND {{paciente_endereco_uf}}]]
group by 1
order by 2 desc
limit 1;