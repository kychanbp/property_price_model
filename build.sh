docker build -t property_price_api .
docker rm property_price_api
docker run -d  --name property_price_api -p 80:80 property_price_api
docker tag property_price_api kychanbp/property_price_api:develop
docker push kychanbp/property_price_api