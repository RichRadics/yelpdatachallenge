


# First, need to load the data into R. It's in a streamed form of JSON, so needs
#   a particular method of loading
library(jsonlite)
library(ggmap)
library(ggplot2)
library(GGally)
library(RColorBrewer)
library(tidyr)
library(dplyr)
# Include my standard function library
source(file="~/R_code/common/rrfuncs.R")
# Optionally clear my environment - makes things clearer
# rm(list=ls())
data_dir = c('~/datasets/yelp/')

# If I've already imported and saved from R, use that - much faster than 
#   reprocessing the JSON
if (file.exists(paste0(data_dir,'yelp_data.Rd')))
{
  load(paste0(data_dir,'yelp_data.Rd'))
} else {
  y.business <- stream_in(file(paste0(data_dir,
                                      "yelp_academic_dataset_business.json")))
  y.checkin <- stream_in(file(paste0(data_dir,
                                     "yelp_academic_dataset_checkin.json")))
  y.review <- stream_in(file(paste0(data_dir,
                                    "yelp_academic_dataset_review.json")))
  y.tip <- stream_in(file(paste0(data_dir,"yelp_academic_dataset_tip.json")))
  y.user <- stream_in(file(paste0(data_dir,"yelp_academic_dataset_user.json")))
  save(list=c('y.business','y.user','y.review','y.tip','y.checkin'), 
       file=paste0(data_dir,'yelp_data.Rd'))
}

# Take a quick look at the structure 
str(y.review)
str(y.business, max.level=1)
# Complex structures... 
# I know that the businesses are located in 10 distinct areas. I have their 
#   names, and the latitude/longitude are in the y.business object
# I'll use k-means clustering to label the groups
# Some geo analysis
# Use kmeans to check state/location based on lat-long
# i have the list of actual locations which the data should be from, so i will
#   seed the kmeans with that
cities<-c('Edinburgh, UK', 'Karlsruhe, Germany', 'Montreal, Canada', 
          'Waterloo, Canada', 'Pittsburgh, PA', 'Charlotte, NC', 
          'Urbana-Champaign, IL', 'Phoenix, AZ', 'Las Vegas, NV', 'Madison, WI')
region.centres<-geocode(cities, source='google')
set.seed(43046721)
myclus<-kmeans(y.business[,c('longitude','latitude')],region.centres)
table(myclus$cluster, y.business$state)
qplot(y.business$location)
# these results show a good grouping, where each state is entirely represented
#   by one cluster, with the exception of two. 
# Cluster 8 and 9 both contain CA - resaonable as both AZ and NV border it. 
#   Cluster 9 seems to contain a rogue SC entry.
subset(y.business, state=='NC' & location=='Las Vegas, NV')$full_address
# A typo!

y.business$location<-cities[myclus$cluster]
# I'll add a distance to the cluster center too - i can then do some analysis
#   of the area covered by each cluster
# This is only for a comparative measure, and the distances /should/ be 
#   relatively short, so i won't go so far as a great circle calculation
t.distance<-y.business[,c('longitude','latitude')]-myclus$centers[
                                                                myclus$cluster,]
y.business$distance.from.centre<-sqrt(rowSums(t.distance**2))
tbl_df(y.business[,c('location','distance.from.centre')]) %>% 
  group_by(location) %>% 
  summarise(mn=mean(distance.from.centre), sd=sd(distance.from.centre))
# This shows that Urbana-Champaign has the tightest group of businesses, while 
#   Phoenix has the most dispersed.
# Lets visualise these on a pair of maps

europe.centre <- colMeans(apply(myclus$centers[1:2,],2,range))
map.europe <- get_map(location = europe.centre, zoom = 6)
map1<-ggmap(map.europe) + 
    geom_point(aes(x=longitude, y=latitude, color=location), 
               data=subset(y.business, 
                        location %in% c('Edinburgh, UK','Karlsruhe, Germany')))

usa.centre <- colMeans(apply(myclus$centers[3:10,],2,range))
map.usa <- get_map(location = usa.centre, zoom = 4)
map2<-ggmap(map.usa) + 
    geom_point(aes(x=longitude, y=latitude, color=location), 
               data=subset(y.business, 
                        !location %in% c('Edinburgh, UK','Karlsruhe, Germany')))

rr.multiplot(map1, map2, cols=2)

# I'd also like to get a primary category for each business - this will also 
# simplify analysis.
# Primary category list from 
#   https://www.yelp.com/developers/documentation/v2/all_category_list
yelp.primary.categories<-
  c('Active Life','Arts & Entertainment','Automotive','Beauty & Spas',
    'Education','Event Planning & Services','Financial Services','Food',
    'Health & Medical','Home Services','Hotels & Travel','Local Flavor',
    'Local Services','Mass Media','Nightlife','Pets','Professional Services',
    'Public Services & Government','Real Estate','Religious Organizations',
    'Restaurants','Shopping')

y.business$primary.categories<-
  lapply(y.business$categories, 
         function (x) intersect(x, yelp.primary.categories))
# 420 combinations of primary categories!!!!!
# so this led me to think more deeply about the categories - and review counts..
# do businesses with more categories have more reviews and/or higher ratings?
cat.dist<-
  data.frame(categories=
               as.factor(
                 sapply(y.business$categories, 
                        function(x) length(unlist(x)))),
             location=y.business$location,
             reviews=y.business$review_count, 
             stars=y.business$stars)
# Build a second variable to hold summaries
cat.dist2<- cat.dist %>% 
            group_by(categories) %>% 
            summarise(mean_reviews=mean(reviews), 
                      mean_stars=mean(stars), 
                      n=n())
rr.multiplot(
  ggplot(data=cat.dist2, aes(x=categories)) + 
    geom_histogram(aes(y=mean_reviews), stat='identity'),
  ggplot(data=cat.dist2, aes(x=categories)) + 
    geom_histogram(aes(y=mean_stars), stat='identity'),
  cols=2)
# There isn't a clear link between the ratings and number of categories, but 
#   there appears to be a clear link between categories and number of reviews


ggplot(data=cat.dist, aes(x=categories, y=reviews)) +
  coord_trans(ytrans='log10') +
  geom_boxplot() +
  geom_jitter(alpha=0.2, aes(color=categories), 
              position=position_jitter(width=.2))+
  scale_colour_brewer('categories', palette='Paired') +
  scale_y_log10()

# Using a log plot of reviews shows there are actually few businesses with very
#   high numbers of reviews. The point density shows the bulk of businesses 
#   have between 2 and 5 categories, and less than 100 reviews

#####   I had hoped to perform more category-based analysis, but with multiple
#####   primary categories, I would have to sift the combinations and derive
#####   so kind of hierarchy to determine a single primary for each business.
#####   Instead, I chose to look into a different view of the data - checkins.
#####   This shows when businesses are visited, by week-hour.
#####   The y.checkindata frame contains my core data - I'll add some of the 
#####   columns from y.business to provide a more useful single frame

y.checkin<-data.frame(flatten(y.checkin))
y.checkin<- 
  y.checkin %>%
  left_join(y.business[,c('business_id','location','review_count',
                          'stars', 'primary.categories')], 
            by = 'business_id')
# Need to gather the checkin_info columns
# I'll split the check-in column names to give hour/day values
y.checkin.long <- 
  y.checkin %>%
  gather(key, value, -location, -stars, -review_count, -type, -business_id, 
         -primary.categories, na.rm=TRUE) %>%
  mutate(checkin.list= strsplit(as.character(key), '\\.')) %>%
  mutate(hr = as.integer(sapply(checkin.list, function (x) x[2])),
         dy = sapply(checkin.list, function (x) x[3])) %>%
  select(-key, -checkin.list, -type, -business_id) 

# Break the primary category list into columns
# This code from http://stackoverflow.com/questions/25347739/r-convert- \\ 
#                     factor-column-to-multiple-boolean-columns
lvl <- unique(unlist(y.checkin.long$primary.categories))      
res <- data.frame(do.call(rbind,lapply(y.checkin.long$primary.categories,
                                       function(x) table(factor(x, levels=lvl)))
                          ), 
                  stringsAsFactors=FALSE)
y.checkin.long<-cbind(y.checkin.long, res)


# This is far too much data for a plot, so sample 10000 rows
heat.data <- 
  y.checkin.long %>%
  #group_by(location) %>%
  filter(value < quantile(value,0.95)) %>%
  #ungroup() %>%
  sample_n(10000)

# Lets look at restaurants, nightlife, and religious organisations
rt.heatmap <- function (l.criteria)
{
  ggplot(subset(heat.data, eval(parse(text=l.criteria))), aes(dy, hr)) + 
    geom_tile(aes(fill = log(value)), colour = "white") + 
    facet_wrap( ~ location) +
    scale_fill_gradient(low = "white", high = "red")
}

rt.heatmap('Restaurants>0')
rt.heatmap('Nightlife>0')
rt.heatmap('Religious.Organizations>0')



# Sunday opening
business2<-y.business[!is.na(y.business$hours),c('location','hours')]
business3<-data.frame(cbind(location=business2$location, 
                          sunday_opening=1*!is.na(business2$hours$Sunday$open)))
business3 %>%
  
  select(location, sunday_opening) %>%
  group_by(location) %>%
  summarise(open_perc = mean(sunday_opening))
  


# This is a vast dataset, so I need to limit myself to a subset - which I'll 
# rollup into a single dataframe. 

# I'm particularly interested in the geographical factors at play, so here I'll 
# build a data frame of review 
#   data with the following data points:
#  y.business :: business_id
#  y.business :: location
#  y.business :: 
y.business[which(myclus$cluster==9&y.business$state=='NC'),]$full_address
# This shows a typo, where NC instead of NV was used. It seems that the 'state' data was taken from the address.
y.business$cluster <- myclus$cluster
table(y.business$cluster)







