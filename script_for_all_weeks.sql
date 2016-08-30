DO $$
DECLARE 
	processing_week date;
BEGIN
    FOR i IN 417..451
    LOOP
	select warehouse.first_within_group(calendar_date) into processing_week from warehouse.date_dim
	where week_ord = i;

	--start insert
	insert into poor_performing_parts_by_mfr_week
	with 
	time_range as (
		select 
		year,
		week_desc ,
		week_ord - 26 as start_week, 
		week_ord - 1  as end_week ,
		week_ord - 4 as current_start,
		week_ord - 1 as current_end ,
		week_ord as processing_week_ord
		from warehouse.date_dim where calendar_date = processing_week 
	),
	past_data as (
		select
			mfr_name ,
			part_number, 
			round (avg(clicks_per_week), 4)  as  mean_clicks_per_week,
			round (stddev_pop(clicks_per_week),4) as sigma_per_week,
			count(week_ord) *100/(select end_week - start_week + 1 from time_range) as confidence 
		from
		(
			select part_number, mfr_name , week_ord , sum(clicks) clicks_per_week
			from warehouse.mfr_part_aggr   mpa
			inner join warehouse.date_dim dd using(date_key)
			where nullif(mpa.part_number,'') is not null
			and week_ord >= (select start_week from time_range )
			and week_ord <= (select end_week from time_range )
			group by mfr_name , part_number, week_ord
		) as innert1
		group by mfr_name, part_number
	),
	current_data_mean as (
		select 
		mfr_name , 
		part_number, 
		recent_mean_clicks_per_week,
		first_val -  last_val as recent_drop_in_clicks
		from (
			select
			mfr_name ,
			part_number, 
			round (avg(clicks_per_week), 4)  as  recent_mean_clicks_per_week ,
			warehouse.last_within_group(clicks_per_week  order by week_ord) as last_val,
			warehouse.first_within_group(clicks_per_week  order by week_ord) as first_val
			from
			(
				select part_number, mfr_name , week_ord , 
				sum(clicks) clicks_per_week 
				from warehouse.mfr_part_aggr   mpa
				inner join warehouse.date_dim dd using(date_key)
				where nullif(mpa.part_number,'') is not null
				and week_ord >= (select current_start from time_range )
				and week_ord <= (select current_end from time_range )
				group by mfr_name , part_number, week_ord
			) as innert1
			group by mfr_name, part_number
		)as innert2
	)
	select 
		(select year  from time_range ) as year, 
		(select week_desc  from time_range ) as week_desc,
		(select processing_week_ord from time_range) as week_ord,
		a.mfr_name , 
		a.part_number, 
		a.mean_clicks_per_week , 
		a.sigma_per_week , 
		a.confidence ,
		b.recent_mean_clicks_per_week , 
		b.recent_drop_in_clicks,
		case when confidence >= 50 then TRUE else FALSE end as confidence_met
	from 
		past_data a inner join current_data_mean b
		on a.mfr_name = b.mfr_name and a.part_number = b.part_number
		where (a.mean_clicks_per_week - a.sigma_per_week) > b.recent_mean_clicks_per_week
		--b.recent_drop_in_clicks > 0 
		;
	
	
	--end insert
	
	--start insert
	insert into trending_parts_by_mfr_week
	(
		year,
		week_desc ,
		week_ord ,
		mfr_name ,
		part_number ,
		historic_mean_clicks ,
		historic_std_dev ,
		confidence ,
		clicks_this_week  ,
		delta_more_than_one_stddev ,
		confidence_met 
	)
	with
	time_range as (
		select 
			year,
			week_desc ,
			week_ord - 26 as oldest_week,
			week_ord - 1 as last_week,
			week_ord curr_week  
		from warehouse.date_dim where calendar_date = CURRENT_DATE
	
	),
	past_data_raw as (
		select 
			mfr_name, week_ord , part_number , sum(clicks) clicks_per_week
		from warehouse.mfr_part_aggr   mpa
		inner join warehouse.date_dim dd using(date_key)
		where nullif(mpa.part_number,'') is not null
		and week_ord >= (select oldest_week from time_range )
		and week_ord < (select last_week  from time_range )
		group by 1 , 2 , 3
	),
	past_data as (
		select 
			mfr_name,
			part_number , 
			round(avg(clicks_per_week),4) as mean_clicks, 
			round(stddev_pop(clicks_per_week),4) as sigma_pop ,
			(count(week_ord) * 100 / (select last_week - oldest_week from time_range))::integer as confidence 
		from past_data_raw
		group by 1 , 2 
	),
	current_data as (
		select 
			mfr_name, part_number , sum(clicks)::integer clicks_this_week
		from warehouse.mfr_part_aggr   mpa
		inner join warehouse.date_dim dd using(date_key)
		where nullif(mpa.part_number,'') is not null
		and week_ord = (select last_week  from time_range )
		group by 1, 2
		
	)
	select 	
		(select year  from time_range ) as year, 
		(select week_desc  from time_range ) as week_desc,
		(select curr_week  from time_range ) as week_ord,
		p.mfr_name , p.part_number ,  
		p.mean_clicks as historic_mean_clicks, 
		p.sigma_pop as historic_std_dev,
		p.confidence,
		c.clicks_this_week,
		( c.clicks_this_week - (p.mean_clicks + p.sigma_pop) ) as delta_more_than_one_stddev ,
		case when p.confidence >=50 then TRUE else FALSE end as confidence_met 
		
	from
		past_data p inner join current_data c on p.mfr_name = c.mfr_name and p.part_number = c.part_number
		where ( c.clicks_this_week - (p.mean_clicks + p.sigma_pop) )   > 0 
		--order by mfr_name , more_than_one_stddev desc 
		and p.mfr_name !~ '\.\.\/'
		;

	--end insert
        raise notice 'Finished %, %',processing_week,i ;
    END LOOP;
    
END$$;