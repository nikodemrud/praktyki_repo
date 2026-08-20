/*Zadanie 1: Filtrowanie (odpowiednik PySparkowego .filter())
Napisz zapytanie, które wyciągnie wszystkie kolumny dla pracowników pracujących na stanowisku (position) "engineer", 
ale tylko tych, którzy zarabiają (grosssalary) więcej niż 6500. 
Dodatkowo niech dane będą posortowane malejąco po zarobkach (od najbogatszego inżyniera do najbiedniejszego).*/

SELECT * FROM dbo.employee_csv
WHERE position = 'engineer' AND grosssalary > 6500
ORDER BY grosssalary DESC;

/*Zadanie 2: Funkcje tekstowe i tworzenie loginów (odpowiednik robienia nowej kolumny w PySparku)
Wyświetl na ekranie stare nazwy (name, surname, stary login) i czwartą kolumnę o nazwie nowy_super_login, 
która będzie wynikiem Twojej manipulacji.*/

SELECT [name], surname, [login], LOWER(CONCAT(SUBSTRING(name, 1, 3), SUBSTRING(surname, 1, 3))) AS primary_key 
FROM dbo.employee_csv;

/*Zadanie 3: Agregacje (odpowiednik PySparkowego .groupBy().agg())
Dla każdego stanowiska (position) wypisz to stanowisko oraz:
Średnią pensję (funkcja AVG) – nazwij tę kolumnę srednia_pensja.
Liczbę osób zatrudnionych na tym stanowisku (funkcja COUNT) .*/

SELECT position, AVG(CAST(grosssalary AS INT)) AS avg_salary , COUNT(position) AS num_people
FROM dbo.employee_csv
GROUP By position;

/*Zadanie 4: Chcemy znaleźć w naszej firmie osoby "przepłacane", to znaczy takie z każdego dowolnego stanowiska, które zarabiają więcej niż wynosi pensja absolutnie każdego analityka ("analyst") z osobna.
Aby to zrobić, w klauzuli WHERE napisz warunek, w którym pensja musi być większa (>) niż słówko ALL, po którym w nawiasach wrzucisz podzapytanie (subquery) 
wyciągające same pensje analityków. Wyciągnij na ekran imię, nazwisko, stanowisko i pensję tych bogaczy.*/

SELECT [name], surname, position, grosssalary
FROM dbo.employee_csv
WHERE CAST(grosssalary AS INT) > ALL (
    SELECT CAST(grosssalary AS INT)
    FROM dbo.employee_csv
    WHERE position = 'analyst'
)

/*Zadanie 5: Logika warunkowa (odpowiednik PySparkowego when(...).otherwise(...))
Szef zarządził podwyżki inflacyjne. Ustalono, że minimalna płaca w firmie wynosi teraz 7500.
Napisz zapytanie używające instrukcji CASE WHEN. Wyciągnij z bazy imię, nazwisko, aktualną pensję (grosssalary) oraz nową kolumnę o nazwie skorygowana_pensja.
Reguła: Jeśli pracownik zarabia mniej niż 7500, w nowej kolumnie wyświetl 7500. W przeciwnym wypadku zostaw jego starą pensję.*/

SELECT [name], surname, position, grosssalary,
CASE
    WHEN CAST(grosssalary AS INT) < 7500 THEN 7500
    ELSE CAST(grosssalary AS INT)
END AS new_gross_salary
FROM dbo.employee_csv

/*Zadanie 6: Funkcje okna / Ranking (odpowiednik PySparkowego dense_rank().over(...))
Polecenie: Wyświetl login, stanowisko (position) i pensję. Dodaj do tego kolumnę ranking_wyplaty, używając funkcji okna DENSE_RANK().
Wskazówka: Musisz "podzielić" dane ze względu na stanowisko (PARTITION BY position) i posortować je wewnątrz tej "paczki" po zarobkach malejąco (ORDER BY CAST(grosssalary AS INT) DESC).*/

SELECT [login], position, grosssalary, DENSE_RANK() OVER(PARTITION BY position ORDER BY CAST(grosssalary AS INT) DESC) AS grosssalary_rank
FROM dbo.employee_csv;

/*Zadanie 7:
Polecenie: Znajdź wszystkich pracowników (wyświetl ich imiona, nazwiska i pozycje), którzy nie są architektami (position != 'architect'), ale zarabiają dokładnie tyle samo, co dowolny (czyli którykolwiek) z architektów.
Wskazówka: Użyj warunku WHERE ... = ANY (...), gdzie w nawiasie umieścisz podzapytanie wyciągające same pensje architektów.*/

SELECT [name], surname, position
FROM dbo.employee_csv
WHERE position != 'architect' 
AND CAST(grosssalary AS INT) = ANY(
    SELECT CAST(grosssalary AS INT)
    FROM dbo.employee_csv
    WHERE position = 'architect' 
)

/*Zadanie 8
Etap A: Użyj słowa kluczowego DISTINCT, aby wyświetlić unikalną listę wszystkich stanowisk (position) w firmie.
Etap B: Napisz zapytanie, które zwróci unikalne zestawienia imienia i nazwiska. Użyj do tego klauzuli GROUP BY zamiast DISTINCT (w SQL dają często ten sam efekt, ale warto znać oba sposoby).*/

SELECT DISTINCT position
FROM dbo.employee_csv;

SELECT [name], surname
FROM dbo.employee_csv
GROUP BY [name], surname 

/* Zadanie 9
Znajdź wszystkich pracowników (wyświetl imię, nazwisko i login), których nazwisko zaczyna się na literę "K", a w swoim loginie mają literkę "o". */

SELECT [name], surname, [login]
FROM dbo.employee_csv
WHERE surname LIKE 'K%' AND [login] LIKE '%o%'

/* Zadanie 10
Wyświetl pary pracowników (imię i nazwisko Pracownika A oraz imię i nazwisko Pracownika B), którzy mają dokładnie tyle samo lat.*/

SELECT 
    a.name AS name_A, 
    a.surname AS surname_A, 
    b.name AS name_B, 
    b.surname AS surname_B, 
    a.age AS same_age
FROM dbo.employee_csv a
JOIN dbo.employee_csv b 
  ON a.age = b.age              
  AND a.login > b.login;        
