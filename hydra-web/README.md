# hydra-web
This folder contains the sources for all compiled web assets.

## How to build
All commands should be run from `hydra-web/`. Remember to run `npm install`, and
install webpack globally:

```
npm install
npm install webpack -g
```

### Development
 - run `npm update` if you change package.json
 - build the assets 1x with `webpack -d`
 - continually build on file changes with `webpack -d --watch`

### Production
 - build the assets with `webpack -p`

## Where do built files go?
To keep existing deploy scripts working, all assets are built to 
`/hydra-main/web/`. 

## What is built?
Currently, only spawn2: `/hydra-main/web/spawn2/build`, but there are plans to
build `query` as well. Static assets are still in `hydra-main/web`, but should
be packaged in the webpack build moving forward.

## What language features are enabled?
 - ES6
 - ES7 decorators
 - Tree shaking