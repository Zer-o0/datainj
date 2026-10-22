-- Вариант: программист-разработчик на Java.
-- Java отдельным словом: JavaScript не совпадает. Роль исключает QA и аналитиков.
SELECT source || ':' || vacancy_id AS id, name, area, experience,
       list_sort(list_distinct(list_filter(
         list_transform(string_split(coalesce(key_skills,''), ';'),
                        lambda skill: lower(trim(skill))), lambda skill: skill <> ''))) AS skills,
       try_cast(salary_from AS DOUBLE) AS salary_from,
       try_cast(salary_to AS DOUBLE) AS salary_to,
       currency, lower(gross) AS gross
FROM read_csv('s3://hh-lake/it_vacancies_2024.csv',
              header=true, all_varchar=true, delim=',', quote='"', escape='"',
              max_line_size=16777216)
WHERE role = 'Программист, разработчик'
  AND regexp_matches(lower(name),'(^|[^a-z0-9а-яё])java([^a-z0-9а-яё]|$)');
