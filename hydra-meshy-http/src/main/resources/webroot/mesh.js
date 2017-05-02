$(function() {
    // set up handlers on button, textarea, table headers
    $("#up").click(function() {
        mesh.navigateUp()
    })
    $("#pwd").keypress(function(e) {
        if (e.keyCode == 13) {
            mesh.pwdUpdated()
            return false
        }
    })
    $("thead th").click(function () {
        console.log($(this).text())
    })
    $("table").click(function (e) {
        var elem = e.target
        if (elem.tagName == "A" && elem.classList.contains("file-link")) {
            mesh.navigateLink(elem)
        }
    })
    $("#close-file").click(function () {
        $("#viewer").hide(0)
        $("table").show(0)
    })

    var path = "/job/*"
    if (localStorage['path']) {
        path = localStorage['path']
    }
    mesh.sort = new Tablesort($("table").get(0))
    mesh.navigateTo(path)
});

var mesh = {
    server: "http://localhost:8080",
    path: ""
};

mesh.pwdUpdated = function() {
    mesh.navigateTo($("#pwd").val())
}

mesh.navigateLink = function(link) {
    var path = link.text
    if (link.classList.contains("dir")) {
        mesh.navigateTo(path + "/*")
    } else {
        mesh.viewFile(path, link.dataset.uuid)
    }
}

mesh.navigateUp = function() {
    var lastSlash = mesh.nthLastIndex(mesh.path, "/", 2)
    if (lastSlash > 0) {
        mesh.navigateTo(mesh.path.substring(0,lastSlash) + "/*")
    }
}

mesh.navigateTo = function(path, isDir) {
    mesh.path = path
    localStorage.path = path
    var table = $("table")
    var spinner = $("#spinner")
    table.hide(0)
    $("#viewer").hide(0)
    spinner.show(0)
    $("#pwd").val(path)
    var params = {path: path}
    $.getJSON(mesh.server + "/list?" + $.param(params), function(data) {
        var tbody = $('<tbody id="files"></tbody>')
        $.each(data, mesh.createRow(tbody))
        $("#files").remove()
        table.append(tbody)
        spinner.hide(0)
        mesh.sort.refresh()
        table.show(0)
    })
}

mesh.shimDirectory = function(file) {
    if (!("isDirectory" in file)) {
        file.isDirectory = mesh.getFilename(file.name).indexOf(".") == -1
    }
}

mesh.getFilename = function(path) {
    var index = path.lastIndexOf("/")
    return path.substring(index + 1, path.length)
}

mesh.createRow = function(tbody) {
    return function(index, file) {
        mesh.shimDirectory(file)
        var type = file.isDirectory ? "dir" : "file"
        var name = '<td><a href="#" class="file-link ' + type + '" data-uuid="' + file.hostUUID + '">' + file.name + "</a></td>"
        var size = "<td>" + mesh.humanReadableSize(file.size) + "</td>"
        var modified = "<td>" + moment(file.lastModified).format('MMMM Do YYYY, h:mm:ss a') + "</td>"
        var host = "<td>" + file.hostUUID.split("-")[0] + "</td>"
        $("<tr>" + name + size + modified + host + "</tr>").appendTo(tbody)
    }
}

mesh.humanReadableSize = function(bytes) {
    var thresh = 1000;
    if(Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }
    var units = ['KiB','MiB','GiB','TiB','PiB','EiB','ZiB','YiB'];
    var u = -1;
    do {
        bytes /= thresh;
        ++u;
    } while(Math.abs(bytes) >= thresh && u < units.length - 1);
    return bytes.toFixed(1)+' '+units[u];
}

mesh.nthLastIndex = function(str, pat, n) {
    i = str.length
    while(n-- && i-- > 0) {
        i = str.lastIndexOf(pat, i)
        if (i <= 0) break;
    }
    return i;
}

mesh.viewFile = function(path, uuid) {
    var content = $("#file-content")
    var url = "get?uuid=" + uuid + "&path=" + encodeURIComponent(path)
    content.attr("data", url)

    var download = mesh.getFilename(path)
    if (download.endsWith(".gz")) {
        download = download.substring(0, download.length - 3)
    }
    $("#download").attr("download", download).attr("href", url)


    $("table").hide(0)
    $("#viewer").show(0)
}