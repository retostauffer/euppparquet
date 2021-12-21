

APP_NAME=euppapi

.PHONY: run
run:
	python manage.py runserver

up:
	docker-compose up

build:
	docker-compose up --build

.PHONY: down
down:
	docker-compose down

# Building css
css: ${APP_NAME}/static/package.json ${APP_NAME}/static/scss/euppapi_bootstrap.scss
	npm run --prefix ./${APP_NAME}/static css:all

# Installing API dependencies via npm
npm: ${APP_NAME}/static/package.json
	#npm cache clean --force
	npm install --prefix ./${APP_NAME}/static
	make css

