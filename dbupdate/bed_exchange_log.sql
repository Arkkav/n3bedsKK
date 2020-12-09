-- nameTail: create_NetricaBedsExchange
-- DEV_VM-1950: create_NetricaBedsExchange
DROP TABLE IF EXISTS `logger`.`NetricaBedsExchange`;
create table if not exists `logger`.`NetricaBedsExchange`
(
    `id`              int auto_increment
        primary key,
   `netrica_id`      varchar(256) not null comment 'id из сервиса netrica',
   `createDatetime`  datetime     not null comment 'Дата создания записи',
   `modifyDatetime`  datetime     not null comment 'Дата изменения записи',
   `netricaBed_code` varchar(32)  not null comment '1.2.643.5.1.13.2.1.1.221',
   `orgStructure_id` int(11)      not null comment 'ЛПУ {OrgStructure}',
   `org_netrica_code` varchar(40)     not null comment '1.2.643.2.69.1.1.1.64',
   `TotalBedCount` int(11) not null default 0 comment 'Общее количество коек',
   `FreeBedCount` int(11) not null default 0 comment 'Общее количество свободных коек',
   `FreeBedCountMale` int(11) not null default 0 comment 'Незанятых мужских коек',
   `FreeBedCountFemale` int(11) not null default 0 comment 'Незанятых женских коек',
   `FreeBedCountChild` int(11) not null default 0 comment 'Незанятых детских коек',
   `AccompPersonCount` int(11) not null default 0 comment 'Количество сопровождающих при больных детях',
   `OccupiedBedCount` int(11) not null default 0 comment 'Количество занятых коек на начало текущих суток',
   `PrevDayOccupiedBedCount` int(11) not null default 0 comment 'Количество занятых коек на начало истекших суток',
   `BedCountOnRepair` int(11) not null default 0 comment 'Количество закрытых на ремонт коек'
) comment 'Таблица результатов обмена данными с коечным фондом КК';

create index `NetricaBedsExchange_orgStructure_id`
    on `logger`.`NetricaBedsExchange` (`orgStructure_id`);

create index `NetricaBedsExchange_netricaBed_code`
    on `logger`.`NetricaBedsExchange` (`netricaBed_code`);

create index `NetricaBedsExchange_netrica_id`
    on `logger`.`NetricaBedsExchange` (`netrica_id`);

create index `NetricaBedsExchange_createDatetime`
    on `logger`.`NetricaBedsExchange` (`createDatetime`);

create index `NetricaBedsExchange_modifyDatetime`
    on `logger`.`NetricaBedsExchange` (`modifyDatetime`);