FROM mcr.microsoft.com/dotnet/core/sdk:3.1 as build
WORKDIR /build
RUN git clone --depth=1 -b separation https://github.com/Coflnet/HypixelSkyblock.git dev
WORKDIR /build/SkyFlipper
COPY SkyFlipper.csproj SkyFlipper.csproj
RUN dotnet restore
COPY . .
RUN dotnet publish -c release

FROM mcr.microsoft.com/dotnet/aspnet:3.1
WORKDIR /app

COPY --from=build /build/SkyFlipper/bin/release/netcoreapp3.1/publish/ .
RUN mkdir -p ah/files

ENTRYPOINT ["dotnet", "SkyFlipper.dll"]

VOLUME /data

