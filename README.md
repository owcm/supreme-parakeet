#surge-backend Platform

Geotrellis based raster analysis application.

- Uses Spray as a REST service
- Takes requests and forwards them on to Spark backend end called sparkhandler
- Includes the GeoTrellis and geotools libraries

Copyright © OpenWhere 2015-2016

**Version 0.1.15**


#Rest API [Version 0.1.15]
<table><thead>
<tr>
<th>Method</th>
<th>Path</th>
<th>Content type</th>
<th>Return</th>
<th>Description</th>
</tr>
</thead><tbody>
<tr>
<td>Post</td>
<td>/api/hillshade?</td>
<td>application/json</td>
<td>200 OK</td>
<td>
Payload:
{
	"extent": [-77.7437210083,39.287014135, -77.6695632935,39.3250026763]
}
</td>
</tr>
<tr>
<td>Post</td>
<td>/api/aspect</td>
<td>application/json</td>
<td>200 OK</td>
<td>
Payload:
{
	"extent": [-77.7437210083,39.287014135, -77.6695632935,39.3250026763]
}
</td>
</tr>
<td>Post</td>
<td>/api/draw</td>
<td>application/json</td>
<td>200 OK</td>
<td>
Payload:
    {
    	"extent": [-77.7437210083,39.287014135, -77.6695632935,39.3250026763]
    }
</td>
</tr>
<tr>
<td>Post</td>
<td>/api/tpi</td>
<td>application/json</td>
<td>200 OK</td>
<td>
Payload:
{
	"extent": [-77.7437210083,39.287014135, -77.6695632935,39.3250026763],
	"parms":{"cellsize":"2"}
}
</td>
</tr>
<tr>
<td>Post</td>
<td>/api/slope</td>
<td>application/json</td>
<td>200 OK</td>
<td>
Payload:
{
	"extent": [-77.7437210083,39.287014135, -77.6695632935,39.3250026763]
}
</td>
</tr>
<tr>
<td>Post</td>
<td>/api/artslope</td>
<td>application/json</td>
<td>200 OK</td>
<td>
Payload:
{
	"extent": [-77.7437210083,39.287014135, -77.6695632935,39.3250026763]
}</td>
</tr>
<tr>
<td>Post</td>
<td>/api/hover</td>
<td>application/json</td>
<td>200 OK</td>
<td>
Payload:
{
	"extent": [-77.7437210083,39.287014135, -77.6695632935,39.3250026763]
}</td>
</tr>
<tr>
<td>Post</td>
<td>/api/viewshed </td>
<td>application/json</td>
<td>200 OK</td>
<td>
	"extent": [-77.7437210083,39.287014135, -77.6695632935,39.3250026763],
	"parms":{"tgtHgt":"2", "orgHgt":"3"}
</td>
</tr>
<tr>
<td>Post</td>
<td>/api/buildobs </td>
<td>application/json</td>
<td>200 OK</td>
<td>
{
    "bareearthlocation":"s3://net-openwhere-surge-resource-uploads/04db19aa-2a4d-49dc-9e5a-5d6477817b42/bareearth.tif",
    "firstreturnlocation":"s3://net-openwhere-surge-resource-uploads/04db19aa-2a4d-49dc-9e5a-5d6477817b42/firstreturns.tif"
}
Can be S3 or HDFS paths
</td>
</tr>
<tr>
<td>Post</td>
<td>/api/buildobslc </td>
<td>application/json</td>
<td>200 OK</td>
<td>
{
    "demmodelURI":<location to dem model to use for building obstruction landcover layer>,
    "landcoverURI":<location to landcover layer to use for building obstruction landcover layer>
}
Can be S3 or HDFS paths
</td>
</tr>
<tr>
<td>Post</td>
<td>/api/buildhover </td>
<td>application/json</td>
<td>200 OK</td>
<td>
{
    "bareearthlocation":"s3://demsrcs/fort-story/sources/dem_bare_50cm_ft_story_tile2.tif",
    "firstreturnlocation":"s3://demsrcs/fort-story/sources/dem_50cm_a1_ft_story_tile2.tif"
}
Can be S3 or HDFS paths
</td>
</tr>
<tr>
<td>Post</td>
<td>/api/upload </td>
<td>application/json</td>
<td>200 OK</td>
<td>
{
	"type":"bareearth",
    "location":"s3://net-openwhere-surge-resource-demsrcs/10m/18STJ5020.tif"
}
</td>
</tr>
<td>Post</td>
<td>/api/hlz
<td>application/json</td>
<td>200 OK</td>
<td>
Payload:
{
	"extent": [-76.0624694824, 36.8856620774, -75.9800720215,36.9449529607],
	"helicopters": [{
		"diameter": "50",
		"approach": "6",
		"dimension": "16",
		"degrees": "Degrees",
		"obstacleRatio": "10",
		"type": "Medium utility and attack helicopters such as the UH-60"
	}, {
		"diameter": "100",
		"approach": "6",
		"dimension": "16",
		"degrees": "Degrees",
		"obstacleRatio": "10",
		"type": "Slingload helicopters and aircraft of an unknown origin."
	}, {
		"diameter": "125",
		"approach": "6",
		"dimension": "16",
		"degrees": "Degrees",
		"obstacleRatio": "10",
		"type": "Slingload long-line operations"
	}, {
		"diameter": "80",
		"approach": "6",
		"dimension": "16",
		"degrees": "Degrees",
		"obstacleRatio": "10",
		"type": "Cargo helicopters such as the CH-47"
	}, {
		"diameter": "35",
		"approach": "6",
		"dimension": "16",
		"degrees": "Degrees",
		"obstacleRatio": "10",
		"type": "Light utility and attack helicopters such as the UH-1H"
	}, {
		"diameter": "25",
		"approach": "6",
		"dimension": "16",
		"degrees": "Degrees",
		"obstacleRatio": "10",
		"type": "Light observation helicopters such as the OH-6"
	}]
}

response
{
      "jobid": "3e643144-93e5-43e5-aa9e-daece5769624"
}
</td>
</tr>
<td>Get</td>
<td>/api/status/{jobid}
<td>application/json</td>
<td>200 OK</td>
<td>{
      "jobId": "928a8734-96a5-47cd-ac25-31c72edf4e3d",
      "status": "Stopped"
    }
</td>
</tr>
<td>Post</td>
<td>/api/cancel/{jobid}
<td>application/json</td>
<td>200 OK</td>
<td>{
      "jobId": "09128150-e888-4259-838a-234a5b7b1873",
      "status": "Stopped"
    }
</td>
</tr>

</tbody></table>

<h1><a id="user-content-change-log" class="anchor" href="#change-log" aria-hidden="true"><svg aria-hidden="true" class="octicon octicon-link" height="16" role="img" version="1.1" viewBox="0 0 16 16" width="16"><path d="M4 9h1v1h-1c-1.5 0-3-1.69-3-3.5s1.55-3.5 3-3.5h4c1.45 0 3 1.69 3 3.5 0 1.41-0.91 2.72-2 3.25v-1.16c0.58-0.45 1-1.27 1-2.09 0-1.28-1.02-2.5-2-2.5H4c-0.98 0-2 1.22-2 2.5s1 2.5 2 2.5z m9-3h-1v1h1c1 0 2 1.22 2 2.5s-1.02 2.5-2 2.5H9c-0.98 0-2-1.22-2-2.5 0-0.83 0.42-1.64 1-2.09v-1.16c-1.09 0.53-2 1.84-2 3.25 0 1.81 1.55 3.5 3 3.5h4c1.45 0 3-1.69 3-3.5s-1.5-3.5-3-3.5z"></path></svg></a>Change Log</h1>

<ul>
<li><strong>0.1.4</strong> Corrected TPI algorithm issues.  Added new color categories for Aspect and TPI</li>
<li><strong>0.1.5</strong> Added /api/hlz endpoints</li>
<li><strong>0.1.7</strong> Moved all ops to /api endpoint structure</li>
<li><strong>0.1.8</strong> HLZ changes </li>
<li><strong>0.1.9</strong> Added Upload Endpoint changes </li>
<li><strong>0.1.10</strong> Added buildobs Endpoint and refactored to better support analytics and pipeline ops </li>
<li><strong>0.1.11</strong> Added error status report back to sparklauncher so job state can be reported. </li>
<li><strong>0.1.12</strong> Final testing and bug fixes buildobs Endpoint.</li>
<li><strong>0.1.13</strong> Added buildobslc build landcover layer</li>
<li><strong>0.1.14</strong> Added endpoint buildhover and hover endpoints</li>
<li><strong>0.1.15</strong> </li>
</ul>