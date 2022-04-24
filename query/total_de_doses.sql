select 
    case
     when vacina_descricao_dose in ('1° Dose','1ª Dose', 'Dose', 'Única') then 'Primeira Dose'
     when vacina_descricao_dose in ('2° Dose','2ª Dose') then 'Segunda Dose'
     when vacina_descricao_dose in ('3ª Dose', 'Dose Adicional', '1º Reforço', 'Reforço') then 'Terceira Dose'
     when vacina_descricao_dose in ('4ª Dose', '2º Reforço') then 'Quarta Dose'
     else vacina_descricao_dose
    end vacina_descricao_dose,
    count(*) quantidade
from datasus_silver_db.datasus
where {{vacina_dataaplicacao}}
[[AND {{paciente_endereco_uf}}]]
group by 1