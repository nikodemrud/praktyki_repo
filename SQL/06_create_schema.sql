-- Create Schema
 IF NOT EXISTS (
    select * from sys.schemas WHERE name = 'bronze'
)
BEGIN
    EXEC('CREATE SCHEMA bronze');
END

IF NOT EXISTS (
    select * from sys.schemas WHERE name = 'silver'
)
BEGIN
    EXEC('CREATE SCHEMA silver');
END
 
IF NOT EXISTS (
    select * from sys.schemas WHERE name = 'gold'
)
BEGIN
    EXEC('CREATE SCHEMA gold');
END