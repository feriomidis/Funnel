with downloads as
(select 0 as step ,'downloads' as name,
                  count(app_download_key) as user_count,
                  0 as count_riddes
                 from app_downloads)
, sighnups as
(select 1 as step,
              'sighnups' as name ,
              count(distinct user_id) as user_count   ,
              0 as count_riddes 
              from signups
              where signup_ts is not null )
,requests as 
(select 2 as step ,
             'request' as name,
             count(distinct user_id) as user_count,
             count(ride_id) as count_riddes 
             from ride_requests
             where request_ts is not null
)
,accepted as 
(select 3 as step ,
             'accepted' as name,
             count( distinct user_id) as user_count,
             count(ride_id) as count_riddes 
              from ride_requests
              where accept_ts is not null
),
compeleted as
(select 4 as step ,
             'compeleted' as name,
              count(distinct user_id)  as user_count,
             count(ride_id) as count_riddes
              from ride_requests
               where pickup_ts is not null and cancel_ts is  null
)
,payment as (select 5 as step ,
             'paid' as name,
             ( select * from(select count(distinct user_id)
                                   from transactions
                                   join ride_requests
                                   using(ride_id)
                                   where charge_status='Approved')sub),
             
             count(ride_id) as count_riddes 
             from transactions
             where transaction_ts is not null
            )
,reviews as(select 6 as step ,'review' as name ,
            count(distinct user_id)  as user_count,
            count(ride_id) as count_riddes from reviews)
,canceled as(select 7 as step ,'canceled' as name,
             count(distinct user_id) as user_count ,
             count(ride_id) as count_riddes 
             from ride_requests
            where cancel_ts is not null)


,funnel as (
  select * from downloads
  union   
  select * from sighnups
  union   
  select * from
  requests
  union   
  select * from
  accepted 
  union   
  select * from
  compeleted
  union   
  select * from
  payment
  union   
  select * from
  reviews
  union   
  select * from
  canceled)
  select * ,coalesce(round
(1.0*(lag(user_count)over(order by step)-user_count)/lag(user_count)
               over(order by step),2),0) as drop_offs
               from funnel
  order by step