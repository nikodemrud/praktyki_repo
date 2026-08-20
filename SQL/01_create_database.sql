-- Create Database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'niru')
    CREATE DATABASE niru