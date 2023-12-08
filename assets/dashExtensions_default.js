window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, latlng) {
            const weather_flag = L.icon({
                iconUrl: `./assets/64x64/${feature.properties.condition_icon_id}.png`,
                iconSize: [64, 64]
            });
            return L.marker(latlng, {
                icon: weather_flag
            });
        },
        function1: function(e, ctx) {
            ctx.map.flyTo([53.726669, -127.647621], 5);
        }
    }
});